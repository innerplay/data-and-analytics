import os
import time
import json
import logging
from datetime import datetime, timezone, timedelta, date
import requests
import pandas as pd
from google.cloud import bigquery, pubsub_v1
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from google.oauth2 import service_account

# ------------------------------------------------------------------------
#  LOAD CONFIGURATION
# ------------------------------------------------------------------------
load_dotenv()

# ------------------------------------------------------------------------
#  PARAMETERS
# ------------------------------------------------------------------------

DAYS_FROM = int(os.getenv("DAYS_FROM", 1))
DAYS_TO = int(os.getenv("DAYS_TO", 0))

META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_IDS").split(',')
API_VERSION = os.getenv("API_VERSION", "v24.0")

BQ_PROJECT = os.getenv("BQ_PROJECT", "analytics-473217")
BQ_DATASET = os.getenv("BQ_DATASET", "hourly")
BQ_TABLE = os.getenv("BQ_TABLE", "meta_insights")
TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
STAGING_TABLE_ID = f"{BQ_PROJECT}.staging.{BQ_DATASET}_{BQ_TABLE}"

REGION = "southamerica-east1"
TOPIC_ID = os.getenv("TOPIC_ID")
PAYLOAD = dict(
	sheet_id = os.getenv("SHEET_ID"),
	worksheet_name = os.getenv("WORKSHEET_NAME"),
	cell_address = os.getenv("CELL_ADDRESS","A1"),
	query_txt = os.getenv("QUERY_TXT")
)
PUBSUB = os.getenv("PUBSUB")

creds_json = os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON']

creds_dict = json.loads(creds_json)
credentials = service_account.Credentials.from_service_account_info(creds_dict)

bq = bigquery.Client(project=BQ_PROJECT, credentials=credentials)

valid_cols = [
	'id_date',
	'hour',
	'account_name',
	'account_id',
	'campaign_name',
	'campaign_id',
	'adset_name',
	'adset_id',
	'ad_name',
	'ad_id',
	'cost',
	'impressions',
	'inline_link_clicks',
	'act_link_click',
	'act_omni_add_to_cart',
	'act_add_to_cart',
	'act_lead',
	'act_add_payment_info',
	'act_omni_purchase',
	'act_purchase',
	'etl_load_date'
]

schema = [
	bigquery.SchemaField("id_date","STRING"),
	bigquery.SchemaField("hour","STRING"),
	bigquery.SchemaField("account_name","STRING"),
	bigquery.SchemaField("account_id","STRING"),
	bigquery.SchemaField("campaign_name","STRING"),
	bigquery.SchemaField("campaign_id","STRING"),
	bigquery.SchemaField("adset_name","STRING"),
	bigquery.SchemaField("adset_id","STRING"),
	bigquery.SchemaField("ad_name","STRING"),
	bigquery.SchemaField("ad_id","STRING"),
	bigquery.SchemaField("cost","FLOAT"),
	bigquery.SchemaField("impressions","INTEGER"),
	bigquery.SchemaField("inline_link_clicks","INTEGER"),
	bigquery.SchemaField("act_link_click","INTEGER"),
	bigquery.SchemaField("act_omni_add_to_cart","INTEGER"),
	bigquery.SchemaField("act_add_to_cart","INTEGER"),
	bigquery.SchemaField("act_lead","INTEGER"),
	bigquery.SchemaField("act_add_payment_info","INTEGER"),
	bigquery.SchemaField("act_omni_purchase","INTEGER"),
	bigquery.SchemaField("act_purchase","INTEGER"),
	bigquery.SchemaField("etl_load_date","TIMESTAMP")
]

logging.basicConfig(
	format="[%(asctime)s] %(levelname)s: %(message)s",
	level=logging.INFO,
)

# ------------------------------------------------------------------------
#  HELPER FUNCTIONS
# ------------------------------------------------------------------------
def date_range(days_from: int, days_to: int) -> list[date]:
	"""Generate a list of dates from (today - days_from) to (today - days_to) inclusive.
	
	days_from: Start offset (e.g., 7 means 7 days ago)
	days_to: End offset (e.g., 0 means today)
	
	Returns list of dates in chronological order (oldest to newest).
	Example: date_range(3, 1) with today=2024-12-10 returns [2024-12-07, 2024-12-08, 2024-12-09]
	"""
	today = date.today()
	start_date = today - timedelta(days=days_from)
	end_date = today - timedelta(days=days_to)
	
	dates = []
	current = start_date
	while current <= end_date:
		dates.append(current)
		current += timedelta(days=1)
	
	return dates


def fetch_meta_data(endpoint: str, params: dict) -> list:
	"""Pull paginated data from Meta API and handle rate‑limit retries."""
	data_rows = []
	retries = 0
	while True:
		r = requests.get(endpoint, params=params)
		data = r.json()

		# Error handling
		if "error" in data:
			msg = data["error"].get("message", "")
			logging.warning(f"API error: {msg}")
			if "request limit" in msg or "rate limit" in msg.lower():
				wait = min(300, (2 ** retries) * 30)
				logging.info(f"Rate-limit hit, sleeping {wait}s...")
				time.sleep(wait)
				retries += 1
				continue
			else:
				break

		data_rows.extend(data.get("data", []))
		next_url = data.get("paging", {}).get("next")
		if not next_url:
			break
		endpoint = next_url
		params = None
	return data_rows


def unpack_actions(actions_list):
	"""Flatten Meta's 'actions' JSON array into {action_type:value} dict."""
	if not isinstance(actions_list, list):
		return {}
	out = {}
	for a in actions_list:
		a_type = a.get("action_type")
		val = a.get("value")
		if a_type:
			try:
				out[a_type] = int(float(val))
			except Exception:
				out[a_type] = 0
	return out

def process_df(rows: list) -> pd.DataFrame:
	if not rows:
		logging.warning("No data returned from API.")
		return pd.DataFrame()

	df = pd.json_normalize(rows)
	# normalize nested actions
	actions_expanded = (
		df["actions"]
		.apply(unpack_actions)
		.apply(pd.Series)
		.add_prefix("act_")
	)
	df = pd.concat([df.drop(columns=["actions"]), actions_expanded], axis=1)

	df.rename(
		columns={
			"hourly_stats_aggregated_by_advertiser_time_zone": "hour",
			"spend": "cost",
		},
		inplace=True,
	)

	df.columns = [c.replace(".", "_") for c in df.columns]

	int_cols = [c for c in df.columns if c.startswith("act")]
	int_cols.extend(['impressions', 'inline_link_clicks'])

	float_cols = [
		"cost"
	]

	for col in int_cols:
		if col in df.columns:
			df[col] = pd.to_numeric(df[col], errors="coerce", downcast="integer").fillna(0).astype("Int64")
		else:
			df[col] = 0
	
	for col in float_cols:
		if col in df.columns:
			df[col] = pd.to_numeric(df.get(col), errors="coerce", downcast="float").fillna(0).astype(float)
		else:
			df[col] = 0.0

	df["id_date"] = pd.to_datetime(df["date_start"], errors="coerce").dt.strftime("%Y%m%d")
	df["etl_load_date"] = datetime.now(timezone.utc)

	df = df[[c for c in valid_cols if c in df.columns]]

	logging.info(f"Processed dataframe shape: {df.shape}")
	return df

# ---------------------------------------------------------------------------
# BIGQUERY HELPERS
# ---------------------------------------------------------------------------
def ensure_bq_resources():
	"""Create all required datasets and tables."""
	for ds_id in [f"{BQ_PROJECT}.{BQ_DATASET}", f"{BQ_PROJECT}.staging"]:
		try:
			bq.get_dataset(ds_id)
		except NotFound:
			dataset = bigquery.Dataset(ds_id)
			dataset.location = REGION
			bq.create_dataset(dataset)
			logging.info("Created dataset: %s", ds_id)
	bq.delete_table(STAGING_TABLE_ID, not_found_ok=True)
	logging.info("Dropped staging table: %s", STAGING_TABLE_ID)
	for table in [TABLE_ID, STAGING_TABLE_ID]:
		try:
			bq.get_table(table)
		except NotFound:
			table = bigquery.Table(table, schema=schema)
			table.time_partitioning = bigquery.TimePartitioning(
				type_=bigquery.TimePartitioningType.DAY, field="etl_load_date"
			)
			bq.create_table(table)
			logging.info("Created table: %s", table)


def append_to_staging(df: pd.DataFrame):
	"""Append a processed DataFrame to staging table."""
	if df.empty:
		logging.info("Empty DataFrame for this date — skipping append.")
		return
	path = "/tmp/meta_staging.parquet"
	df.to_parquet(path, index=False)

	job_config = bigquery.LoadJobConfig(
		source_format=bigquery.SourceFormat.PARQUET,
		write_disposition="WRITE_APPEND",
		schema_update_options=["ALLOW_FIELD_ADDITION"],
	)

	with open(path, "rb") as f:
		bq.load_table_from_file(f, STAGING_TABLE_ID, job_config=job_config).result()

	logging.info("Appended %s rows into staging.", len(df))

def merge_staging_to_prod():	
	"""Merge full staging dataset into production table."""
	query = f"""
	MERGE `{TABLE_ID}` T
	USING `{STAGING_TABLE_ID}` S
	ON
		T.id_date = S.id_date AND
		T.hour = S.hour AND
		T.account_id = S.account_id AND
		T.campaign_id = S.campaign_id AND
		T.adset_id = S.adset_id AND
		T.ad_id = S.ad_id
	WHEN MATCHED THEN UPDATE SET
		T.account_name = S.account_name,
		T.campaign_name = S.campaign_name,
		T.adset_name = S.adset_name,
		T.ad_name = S.ad_name,
		T.cost = S.cost,
		T.impressions = S.impressions,
		T.inline_link_clicks = S.inline_link_clicks,
		T.act_link_click = S.act_link_click,
		T.act_omni_add_to_cart = S.act_omni_add_to_cart,
		T.act_add_to_cart = S.act_add_to_cart,
		T.act_lead = S.act_lead,
		T.act_add_payment_info = S.act_add_payment_info,
		T.act_omni_purchase = S.act_omni_purchase,
		T.act_purchase = S.act_purchase,
		T.etl_load_date = S.etl_load_date
	WHEN NOT MATCHED THEN INSERT ROW
	"""
	bq.query(query).result()
	logging.info("Merged staging into production successfully.")
	bq.delete_table(STAGING_TABLE_ID, not_found_ok=True)
	logging.info("Dropped staging table: %s", STAGING_TABLE_ID)

def publish_message(payload: dict):
	"""Publishes a message with the given payload to Pub/Sub."""
	publisher = pubsub_v1.PublisherClient(credentials=credentials)
	topic_path = publisher.topic_path(BQ_PROJECT, TOPIC_ID)

	data = json.dumps(payload).encode("utf-8")
	future = publisher.publish(topic_path, data)
	message_id = future.result()
	print(f"Published message ID: {message_id}")

# ------------------------------------------------------------------------
#  MAIN
# ------------------------------------------------------------------------
def main():
	"""Load all days into one staging table, then merge once."""
	ensure_bq_resources()

	total_rows = 0
	dates = date_range(DAYS_FROM, DAYS_TO)
	print(dates)

	for account in AD_ACCOUNT_ID:
		for date in dates:
			endpoint = f"https://graph.facebook.com/{API_VERSION}/{account}/insights"
			params = {
				"fields": (
					"date_start,account_name,account_id,campaign_name,campaign_id,adset_name,adset_id,ad_name,"
					"ad_id,spend,impressions,inline_link_clicks,actions"
				),
				"level": "ad",
				"breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
				"time_increment": "1",
				"time_range": json.dumps({"since": date.strftime("%Y-%m-%d"), "until": date.strftime("%Y-%m-%d")}),
				"limit": 500,
				"access_token": META_ACCESS_TOKEN,
			}

			logging.info(f"Fetching Meta Ads data for {account} - {date} ...")
			rows = fetch_meta_data(endpoint, params)
			logging.info(f"Fetched {len(rows)} rows for {account} - {date}")
			df = process_df(rows)
			append_to_staging(df)
			total_rows += len(df)

	logging.info("Total rows loaded into staging: %s",total_rows)
	merge_staging_to_prod()
	if PUBSUB:
		publish_message(PAYLOAD)

if __name__ == "__main__":
	main()