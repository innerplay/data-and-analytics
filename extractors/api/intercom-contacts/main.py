import io
import json
import logging
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from threading import Lock

import fastavro
import requests
from google.cloud import storage

# ========= LOGGING =========
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s [%(levelname)s] %(message)s",
	datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ========= CONFIG =========
TOKEN = os.environ.get("INTERCOM_TOKEN")
API_VERSION = os.environ.get("API_VERSION", "2.14")
DAYS_FROM = int(os.environ.get("DAYS_FROM", "0"))  # timedelta start (e.g., 7 = 7 days ago)
DAYS_TO = int(os.environ.get("DAYS_TO", "0"))      # timedelta end (e.g., 0 = today)
INTERCOM_URL = "https://api.intercom.io/contacts/search"

GCS_BUCKET = os.environ.get("GCS_BUCKET", "data-innerai")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "data-and-analytics/raw-data/intercom/contacts")

UPLOAD_WORKERS = 10

# Shared state
upload_queue = Queue()
upload_counter = 0
counter_lock = Lock()

# Prebuilt headers
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


def sanitize(obj, key: str = None):
	"""Recursively sanitize keys and remove invisible characters from strings."""
	if key == "data":
		return json.dumps(obj, ensure_ascii=False) if not isinstance(obj, str) else obj
	if isinstance(obj, dict):
		return {sanitize_key(k): sanitize(v, k) for k, v in obj.items()}
	if isinstance(obj, list):
		return [sanitize(item) for item in obj]
	if isinstance(obj, str):
		return re.sub(r'[\u200B-\u200F\u2028-\u202F\uFEFF]', '', obj)
	return obj


def timedelta_range(days_from: int, days_to: int) -> list[int]:
	"""Generate a range of day offsets from days_from to days_to (inclusive).
	
	days_from: Start offset (e.g., 7 means 7 days ago)
	days_to: End offset (e.g., 0 means today)
	
	Returns list of offsets in descending order (oldest to newest).
	"""
	if days_from >= days_to:
		return list[int](range(days_from, days_to - 1, -1))
	else:
		return list[int](range(days_from, days_to + 1))


def get_date_from_delta(delta_days: int) -> str:
	"""Get ISO date string for a given day offset from today."""
	target = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=delta_days)
	return target.isoformat(timespec="seconds")


def build_payload(date: str, starting_after: str = None) -> dict:
	"""Build Intercom API search payload for a specific day."""
	payload = {
		"query": {
			"operator": "AND",
			"value": [{"field": "updated_at", "operator": "=", "value": unix(date)}],
		},
		"pagination": {"per_page": 50}
	}
	if starting_after:
		payload["pagination"]["starting_after"] = starting_after
	return payload


# ========= UPLOAD WORKER =========

def upload_worker(bucket, stop_event):
	"""Upload files from queue to GCS."""
	global upload_counter
	
	while not stop_event.is_set() or not upload_queue.empty():
		try:
			object_name, content = upload_queue.get(timeout=0.5)
		except Empty:
			continue
		
		try:
			bucket.blob(object_name).upload_from_string(content, content_type="avro/binary")
			with counter_lock:
				upload_counter += 1
				logger.info(f"[Upload {upload_counter}] → {object_name.split('/')[-1]}")
		except Exception as e:
			logger.error(f"Upload failed for {object_name}: {e}")
		finally:
			upload_queue.task_done()


def flatten_for_avro(data: dict) -> dict:
	"""Flatten all values to strings for consistent Avro schema."""
	result = {}
	for k, v in data.items():
		if v is None:
			result[k] = None
		elif isinstance(v, (dict, list)):
			result[k] = json.dumps(v, ensure_ascii=False) if v else None
		else:
			result[k] = str(v)
	return result


def to_avro_bytes(data: dict) -> bytes:
	"""Convert a dict to Avro bytes with dynamic schema."""
	flat_data = flatten_for_avro(data)
	
	# Build schema dynamically from the data keys (all nullable strings)
	schema = {
		"type": "record",
		"name": "IntercomContact",
		"fields": [
			{"name": k, "type": ["null", "string"], "default": None}
			for k in flat_data.keys()
		]
	}
	
	buffer = io.BytesIO()
	fastavro.writer(buffer, fastavro.parse_schema(schema), [flat_data])
	return buffer.getvalue()


# ========= FETCH =========

def fetch_day(date: str) -> int:
	"""Fetch all contacts for a day and queue for upload."""
	starting_after = None
	total = 0
	page = 0

	logger.info(f"Fetching: {date}")

	while True:
		page += 1
		resp = requests.post(INTERCOM_URL, json=build_payload(date, starting_after), headers=HEADERS, timeout=60)
		resp.raise_for_status()
		data = resp.json()
		contacts = data.get("data", [])

		for contact in contacts:
			contact_id = contact.get("id", "unknown")
			created_at = contact.get("created_at")
			partition = get_partition_path(iso(created_at)) if created_at else "unknown"
			
			upload_queue.put((
				f"{GCS_PREFIX}/{partition}/{contact_id}.avro",
				to_avro_bytes(sanitize(contact))
			))

		total += len(contacts)
		if contacts:
			logger.info(f"Queued {len(contacts)} files from page {page}")

		next_page = data.get("pages", {}).get("next", {})
		starting_after = next_page.get("starting_after") if next_page else None
		if not starting_after:
			break

	logger.info(f"Done fetching {date}: {total} contacts")
	return total

# ========= MAIN FUNCTIONS =========

def stream_to_gcs(date: str):
	"""Fetch contacts for a day from Intercom and upload to GCS."""
	global upload_counter
	upload_counter = 0

	bucket = storage.Client().bucket(GCS_BUCKET)
	stop_event = threading.Event()
	
	logger.info(f"Starting: {date} with {UPLOAD_WORKERS} upload workers")

	# Start upload workers
	upload_threads = [
		threading.Thread(target=upload_worker, args=(bucket, stop_event), daemon=True)
		for _ in range(UPLOAD_WORKERS)
	]
	for t in upload_threads:
		t.start()

	# Fetch all contacts for the day
	total = fetch_day(date)

	# Wait for uploads and cleanup
	logger.info("Fetch done. Waiting for uploads...")
	upload_queue.join()
	stop_event.set()
	for t in upload_threads:
		t.join(timeout=2)

	logger.info(f"Done! {total} contacts in {upload_counter} files.")

if __name__ == "__main__":
	for delta in timedelta_range(DAYS_FROM, DAYS_TO):
		date = get_date_from_delta(delta)
		logger.info(f"Processing delta={delta} days ago → {date}")
		stream_to_gcs(date)