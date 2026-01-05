import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from threading import Lock

import requests
from google.cloud import bigquery, storage

# ========= LOGGING =========
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s [%(levelname)s] %(message)s",
	datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ========= CONFIG =========
TOKEN = os.environ.get("INTERCOM_TOKEN", "dG9rOmQ4ZjJkN2RjXzM0NGNfNDEwZl84NGQ5Xzk5MDg4ODdkOTdiYjoxOjA=")
API_VERSION = os.environ.get("API_VERSION", "2.14")
DATE_FROM = os.environ.get("DATE_FROM", "2025-09-15T00:00:00Z")
DATE_TO = os.environ.get("DATE_TO", "2025-10-22T00:00:00Z")
INTERCOM_URL = "https://api.intercom.io/contacts/search"

GCS_BUCKET = os.environ.get("GCS_BUCKET", "data-innerai")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "data-and-analytics/raw-data/intercom/contacts")

BQ_PROJECT = os.environ.get("BQ_PROJECT", "analytics-473217")
BQ_DATASET = os.environ.get("BQ_DATASET", "staging")
BQ_TABLE = os.environ.get("BQ_TABLE", "intercom_contacts")

FETCH_WORKERS = 4
UPLOAD_WORKERS = 20

# Shared state
upload_queue = Queue()
upload_counter = 0
counter_lock = Lock()

# Prebuilt headers (constant)
HEADERS = {
	"Content-Type": "application/json",
	"Intercom-Version": API_VERSION,
	"Authorization": f"Bearer {TOKEN}",
}

# ========= HELPERS =========

def parse_iso(iso_string: str) -> datetime:
	"""Parse ISO string to datetime, handling 'Z' suffix."""
	return datetime.fromisoformat(iso_string.replace("Z", "+00:00"))


def unix(iso_string: str) -> int:
	"""Convert ISO string to Unix timestamp."""
	return int(parse_iso(iso_string).timestamp())


def iso(timestamp: int) -> str:
	"""Convert Unix timestamp to ISO string."""
	return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def get_partition_path(iso_string: str) -> str:
	"""Get YYYY/MM/DD partition path from ISO string."""
	dt = parse_iso(iso_string)
	return f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"


def sanitize_key(key: str) -> str:
	"""Sanitize key for BigQuery compatibility."""
	key = re.sub(r'[\s\-]+', '_', key)
	key = re.sub(r'[^a-zA-Z0-9_]', '', key)
	return ('_' + key if key and key[0].isdigit() else key).lower()


def sanitize(obj):
	"""Recursively sanitize keys and remove invisible characters from strings."""
	if isinstance(obj, dict):
		return {sanitize_key(k): sanitize(v) for k, v in obj.items()}
	if isinstance(obj, list):
		return [sanitize(item) for item in obj]
	if isinstance(obj, str):
		return re.sub(r'[\u200B-\u200F\u2028-\u202F\uFEFF]', '', obj)
	return obj


def date_range(date_from: str, date_to: str) -> list[tuple[str, str]]:
	"""Generate daily (start, end) tuples between dates."""
	start, end = parse_iso(date_from), parse_iso(date_to)
	days = []
	while start < end:
		next_day = min(start + timedelta(days=1), end)
		days.append((start.isoformat(timespec="seconds"), next_day.isoformat(timespec="seconds")))
		start = next_day
	return days


def split_date_range(date_from: str, date_to: str, num_chunks: int) -> list[tuple[str, str]]:
	"""Split date range into equal chunks for parallel processing."""
	start, end = parse_iso(date_from), parse_iso(date_to)
	chunk_seconds = (end - start).total_seconds() / num_chunks
	
	return [
		(
			(start + timedelta(seconds=i * chunk_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ"),
			(start + timedelta(seconds=(i + 1) * chunk_seconds) if i < num_chunks - 1 else end).strftime("%Y-%m-%dT%H:%M:%SZ")
		)
		for i in range(num_chunks)
	]


def build_payload(date_from: str, date_to: str, starting_after: str = None) -> dict:
	"""Build Intercom API search payload."""
	payload = {
		"query": {
			"operator": "AND",
			"value": [
				{"field": "updated_at", "operator": ">", "value": str(unix(date_from))},
				{"field": "updated_at", "operator": "<", "value": str(unix(date_to))},
			],
		},
		"pagination": {"per_page": 50}
	}
	if starting_after:
		payload["pagination"]["starting_after"] = starting_after
	return payload


# ========= WORKERS =========

def upload_worker(bucket, stop_event):
	"""Upload files from queue to GCS."""
	global upload_counter
	
	while not stop_event.is_set() or not upload_queue.empty():
		try:
			object_name, content = upload_queue.get(timeout=0.5)
		except Empty:
			continue
		
		try:
			bucket.blob(object_name).upload_from_string(content, content_type="application/x-ndjson")
			with counter_lock:
				upload_counter += 1
				logger.info(f"[Upload {upload_counter}] → {object_name.split('/')[-1]}")
		except Exception as e:
			logger.error(f"Upload failed for {object_name}: {e}")
		finally:
			upload_queue.task_done()


def fetch_chunk(chunk_id: int, date_from: str, date_to: str) -> int:
	"""Fetch contacts for a date range chunk and queue for upload."""
	starting_after = None
	chunk_total = 0
	local_page = 0

	logger.info(f"[Fetch {chunk_id}] Starting: {date_from} → {date_to}")

	while True:
		local_page += 1
		resp = requests.post(
			INTERCOM_URL,
			json=build_payload(date_from, date_to, starting_after),
			headers=HEADERS,
			timeout=60
		)
		resp.raise_for_status()
		data = resp.json()
		contacts = data.get("data", [])

		for contact in contacts:
			contact_id = contact.get("id", "unknown")
			created_at = contact.get("created_at")
			partition = get_partition_path(iso(created_at)) if created_at else "unknown"
			
			upload_queue.put((
				f"{GCS_PREFIX}/{partition}/{contact_id}.ndjson",
				json.dumps(sanitize(contact), ensure_ascii=False)
			))

		chunk_total += len(contacts)
		if contacts:
			logger.info(f"[Fetch {chunk_id}] Queued {len(contacts)} files from page {local_page}")

		# Check for next page
		next_page = data.get("pages", {}).get("next", {})
		starting_after = next_page.get("starting_after") if next_page else None
		if not starting_after:
			break

	logger.info(f"[Fetch {chunk_id}] Done! {chunk_total} contacts")
	return chunk_total


# ========= GCS TO BIGQUERY =========

BQ_SCHEMA = [
	bigquery.SchemaField("id", "STRING"),
	bigquery.SchemaField("external_id", "STRING"),
	bigquery.SchemaField("created_at", "INTEGER"),
	bigquery.SchemaField("updated_at", "INTEGER"),
	bigquery.SchemaField("email", "STRING"),
	bigquery.SchemaField("phone", "STRING"),
	bigquery.SchemaField("name", "STRING"),
	bigquery.SchemaField("last_seen_at", "INTEGER"),
	bigquery.SchemaField("last_replied_at", "INTEGER"),
	bigquery.SchemaField("last_contacted_at", "INTEGER"),
	bigquery.SchemaField("custom_attributes", "JSON"),
]


def load_gcs_to_bigquery(partition: str, schema: list = None):
	"""Load NDJSON files from GCS partition into BigQuery."""
	gcs_uri = f"gs://{GCS_BUCKET}/{GCS_PREFIX}/{partition}/*.ndjson"
	table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
	
	logger.info(f"Loading: {gcs_uri} → {table_id}")
	
	bq_client = bigquery.Client(project=BQ_PROJECT)
	job_config = bigquery.LoadJobConfig(
		source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
		write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
		max_bad_records=0,
		**({"schema": schema, "ignore_unknown_values": True} if schema else {"autodetect": True})
	)
	
	load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
	logger.info(f"Started job: {load_job.job_id}")
	load_job.result()
	
	table = bq_client.get_table(table_id)
	logger.info(f"Done! Loaded {load_job.output_rows} rows. Table now has {table.num_rows} total rows.")
	return load_job


# ========= MAIN FUNCTIONS =========

def stream_to_gcs_ndjson(date_from: str, date_to: str):
	"""Fetch contacts from Intercom and upload to GCS in parallel."""
	global upload_counter
	upload_counter = 0

	client = storage.Client()
	bucket = client.bucket(GCS_BUCKET)
	chunks = split_date_range(date_from, date_to, FETCH_WORKERS)
	
	logger.info(f"Starting: {FETCH_WORKERS} fetch workers + {UPLOAD_WORKERS} upload workers")
	logger.info(f"Date range: {date_from} → {date_to}")

	stop_event = threading.Event()
	
	# Start upload workers
	upload_threads = [
		threading.Thread(target=upload_worker, args=(bucket, stop_event), daemon=True)
		for _ in range(UPLOAD_WORKERS)
	]
	for t in upload_threads:
		t.start()

	# Run fetch workers
	total_contacts = 0
	with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
		futures = [executor.submit(fetch_chunk, i + 1, c[0], c[1]) for i, c in enumerate(chunks)]
		for future in as_completed(futures):
			try:
				total_contacts += future.result()
			except Exception as e:
				logger.error(f"Fetch failed: {e}")

	# Wait for uploads and cleanup
	logger.info("Fetches done. Waiting for uploads...")
	upload_queue.join()
	stop_event.set()
	for t in upload_threads:
		t.join(timeout=2)

	logger.info(f"Done! {total_contacts} contacts in {upload_counter} files.")


def run_full_pipeline(date_from: str, date_to: str):
	"""Complete ETL: Intercom → GCS → BigQuery"""
	logger.info("=" * 50)
	logger.info("STEP 1: Fetch from Intercom → GCS")
	logger.info("=" * 50)
	stream_to_gcs_ndjson(date_from, date_to)
	
	logger.info("=" * 50)
	logger.info("STEP 2: Load GCS → BigQuery")
	logger.info("=" * 50)
	load_gcs_to_bigquery(get_partition_path(date_from))
	
	logger.info("Pipeline complete!")


if __name__ == "__main__":
	for start, end in date_range(DATE_FROM, DATE_TO):
		stream_to_gcs_ndjson(start, end)
