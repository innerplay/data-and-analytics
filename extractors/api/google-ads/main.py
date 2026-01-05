import os
import json
import sys
import logging
from datetime import datetime, timezone, date, timedelta
from typing import Optional, List, Dict, Any

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# BigQuery configuration
BQ_PROJECT = os.getenv("BQ_PROJECT", "analytics-473217")
BQ_DATASET = os.getenv("BQ_DATASET", "hourly")
BQ_TABLE = os.getenv("BQ_TABLE", "google_ads")
BQ_REGION = os.getenv("BQ_REGION", "southamerica-east1")

TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
STAGING_TABLE_ID = f"{BQ_PROJECT}.staging.{BQ_TABLE}"
DAYS_FROM = int(os.getenv("DAYS_FROM", 1))
DAYS_TO = int(os.getenv("DAYS_TO", 0))

DATE_FROM = date.today() - timedelta(days=DAYS_FROM)
DATE_TO = date.today() - timedelta(days=DAYS_TO)

date_range = f"{DATE_FROM},{DATE_TO}"

# BigQuery client
bq = bigquery.Client(project=BQ_PROJECT)

# BigQuery schema
SCHEMA = [
	bigquery.SchemaField("id_date", "STRING"),
	bigquery.SchemaField("date", "DATE"),
	bigquery.SchemaField("hour", "INTEGER"),
	bigquery.SchemaField("campaign_id", "INTEGER"),
	bigquery.SchemaField("campaign_name", "STRING"),
	bigquery.SchemaField("impressions", "INTEGER"),
	bigquery.SchemaField("clicks", "INTEGER"),
	bigquery.SchemaField("cost", "FLOAT"),
	bigquery.SchemaField("conversions", "FLOAT"),
	bigquery.SchemaField("all_conversions", "FLOAT"),
	bigquery.SchemaField("etl_load_date", "TIMESTAMP")
]


def get_env_var(name: str, required: bool = True, default: Optional[str] = None) -> str:
	value = os.getenv(name, default)
	if required and not value:
		logging.error(f"Missing required environment variable: {name}")
		sys.exit(1)
	return value


def build_query(date_range: str) -> str:
	"""
	Build a GAQL query based on a date range.

	date_range can be:
	  - Named range: e.g. LAST_30_DAYS, YESTERDAY, LAST_7_DAYS, etc.
	  - Explicit range: e.g. 2024-01-01,2024-01-31
	"""
	if "," in date_range:
		# Expect "YYYY-MM-DD,YYYY-MM-DD"
		start_date, end_date = [x.strip() for x in date_range.split(",", 1)]
		where_clause = f"WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'"
	else:
		where_clause = f"WHERE segments.date DURING {date_range}"

	query = f"""
		SELECT 
			metrics.cost_micros, 
			metrics.impressions, 
			metrics.clicks, 
			segments.date, 
			segments.hour, 
			campaign.id, 
			campaign.name, 
			metrics.all_conversions, 
			metrics.conversions 
			FROM campaign
		{where_clause}
		ORDER BY
			segments.date,
			segments.hour,
			campaign.id
	"""
	return query


def fetch_campaign_metrics(client: GoogleAdsClient, customer_id: str, date_range: str) -> List[Dict[str, Any]]:
	ga_service = client.get_service("GoogleAdsService")

	query = build_query(date_range)
	results = []
	etl_load_date = datetime.now(timezone.utc).isoformat()

	try:
		response = ga_service.search(
			customer_id=customer_id,
			query=query,
		)

		for row in response:
			cost = row.metrics.cost_micros / 1_000_000.0
			date_str = row.segments.date
			hour = row.segments.hour
			
			results.append({
				"id_date": date_str.replace("-", ""),
				"date": date_str,
				"hour": hour,
				"campaign_id": row.campaign.id,
				"campaign_name": row.campaign.name,
				"impressions": row.metrics.impressions,
				"clicks": row.metrics.clicks,
				"cost": cost,
				"conversions": row.metrics.conversions,
				"all_conversions": row.metrics.all_conversions,
				"etl_load_date": etl_load_date
			})
	except GoogleAdsException as ex:
		error_payload = {
			"request_id": ex.request_id,
			"status": ex.error.code().name,
			"errors": [
				{
					"code": str(err.error_code),
					"message": err.message,
				}
				for err in ex.failure.errors
			],
		}
		logging.error(json.dumps({"error": error_payload}, indent=2))
		sys.exit(1)

	logging.info(f"Fetched {len(results)} rows from Google Ads API")
	return results


# ---------------------------------------------------------------------------
# BIGQUERY HELPERS
# ---------------------------------------------------------------------------
def ensure_bq_resources():
	"""Create dataset and tables if they don't exist."""
	# Create main dataset
	for ds_id in [f"{BQ_PROJECT}.{BQ_DATASET}", f"{BQ_PROJECT}.staging"]:
		try:
			bq.get_dataset(ds_id)
		except NotFound:
			dataset = bigquery.Dataset(ds_id)
			dataset.location = BQ_REGION
			bq.create_dataset(dataset)
			logging.info(f"Created dataset: {ds_id}")

	# Drop and recreate staging table
	bq.delete_table(STAGING_TABLE_ID, not_found_ok=True)
	logging.info(f"Dropped staging table: {STAGING_TABLE_ID}")

	# Create tables if they don't exist
	for table_id in [TABLE_ID, STAGING_TABLE_ID]:
		try:
			bq.get_table(table_id)
		except NotFound:
			table = bigquery.Table(table_id, schema=SCHEMA)
			table.time_partitioning = bigquery.TimePartitioning(
				type_=bigquery.TimePartitioningType.DAY,
				field="etl_load_date"
			)
			bq.create_table(table)
			logging.info(f"Created table: {table_id}")


def load_to_staging(rows: List[Dict[str, Any]]):
	"""Load rows to staging table."""
	if not rows:
		logging.info("No rows to load — skipping.")
		return

	job_config = bigquery.LoadJobConfig(
		source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
		write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
		schema=SCHEMA,
	)

	job = bq.load_table_from_json(rows, STAGING_TABLE_ID, job_config=job_config)
	job.result()  # Wait for completion
	logging.info(f"Loaded {len(rows)} rows to staging table.")


def merge_staging_to_prod():
	"""Merge staging data into production table (upsert on id_date, hour, campaign_id)."""
	query = f"""
	MERGE `{TABLE_ID}` T
	USING `{STAGING_TABLE_ID}` S
		ON T.id_date = S.id_date
		AND T.hour = S.hour
		AND T.campaign_id = S.campaign_id
	WHEN MATCHED THEN UPDATE SET
		T.impressions = S.impressions,
		T.clicks = S.clicks,
		T.cost = S.cost,
		T.conversions = S.conversions,
		T.all_conversions = S.all_conversions,
		T.etl_load_date = S.etl_load_date
	WHEN NOT MATCHED THEN INSERT ROW
	"""
	job = bq.query(query)
	job.result()
	logging.info("Merged staging data into production table.")
	bq.delete_table(STAGING_TABLE_ID, not_found_ok=True)
	logging.info(f"Dropped staging table: {STAGING_TABLE_ID}")


def main():
	# Required env vars
	customer_id = get_env_var("GOOGLE_ADS_CUSTOMER_ID", required=False, default="3308845392")
	# Optionally override config path
	config_path = os.getenv("GOOGLE_ADS_CONFIG", "./google_ads.yaml")

	logging.info(f"Starting Google Ads extraction for customer {customer_id}, date range: {date_range}")

	# Initialize Google Ads client
	if config_path:
		client = GoogleAdsClient.load_from_storage(path=config_path)
	else:
		client = GoogleAdsClient.load_from_storage()

	# Fetch data from Google Ads
	data = fetch_campaign_metrics(client, customer_id, date_range)

	if not data:
		logging.info("No data returned from Google Ads API.")
		return

	# Load to BigQuery
	logging.info("Ensuring BigQuery resources exist...")
	ensure_bq_resources()

	logging.info("Loading data to staging table...")
	load_to_staging(data)

	logging.info("Merging staging to production...")
	merge_staging_to_prod()

	logging.info(f"Successfully loaded {len(data)} rows to {TABLE_ID}")


if __name__ == "__main__":
	main()