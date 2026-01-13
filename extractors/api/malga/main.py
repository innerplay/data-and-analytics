import requests
from datetime import datetime, timedelta, timezone
import os
import time
import logging
import pandas as pd
from io import BytesIO, StringIO
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
DAYS_FROM = int(os.getenv("DAYS_FROM", 1))
DAYS_TO = int(os.getenv("DAYS_TO", 0))
MALGA_CLIENT_ID = os.environ["MALGA_CLIENT_ID"]
MALGA_API_KEY = os.environ["MALGA_API_KEY"]
EMAIL = os.environ["EMAIL"]

base_url = "https://api.malga.io/v1/reports"

headers = {
    "accept-language": "en",
    "X-User-Timezone": "America/Sao_Paulo",
    "X-Client-Id": MALGA_CLIENT_ID,
    "X-Api-Key": MALGA_API_KEY,
    "Content-Type": "application/json"
}

files = []

for days in range(DAYS_FROM, DAYS_TO - 1, -1):
    try:
        date = datetime.now(timezone.utc) - timedelta(days=days)
        payload = {
            "sendTo": EMAIL,
            "type": "transactionsHistory",
            "fields": "all",
            "filters": {
                "transactionCreatedAt": {
                    "gte": date.strftime("%Y-%m-%dT00:00:00Z"),
                    "lte": date.strftime("%Y-%m-%dT23:59:59Z")
                }
            }
        }

        response = requests.post(base_url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        files.append(data.get("id", ''))
    except Exception as e:
        logger.error(f"Failed to request report: {e}")

time.sleep(5)

dataframes = []

for file in files:
    file_url = f"https://api.malga.io/v1/reports/{file}/files/1"

    response = requests.get(file_url, headers={"X-Client-Id": MALGA_CLIENT_ID, "X-Api-Key": MALGA_API_KEY})
    if response.status_code == 200:
        file_content = response.content

        df = None
        try:
            df = pd.read_csv(StringIO(file_content.decode('utf-8')))
        except Exception:
            try:
                df = pd.read_excel(BytesIO(file_content))
            except Exception as e:
                logger.error(str(e))

        if df is not None:
            dataframes.append(df)
    else:
        logger.warning(f"Unexpected status code {response.status_code} for file {file}")

creds_json = os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON']

creds_dict = json.loads(creds_json)
credentials = service_account.Credentials.from_service_account_info(creds_dict)

project_id = creds_dict['project_id']
bq_client = bigquery.Client(credentials=credentials, project=project_id)

if dataframes:
    all_data = pd.concat(dataframes, ignore_index=True)
    # Normalize all column names: lowercase and replace spaces with underscores
    all_data.columns = [re.sub(r'[^a-zA-Z0-9]+', '_', col.strip().lower()) for col in all_data.columns]
else:
    all_data = pd.DataFrame()

if not all_data.empty:
    table_id = f"{project_id}.raw_data.malga_transactions"
    staging_table_id = f"{project_id}.staging.malga_transactions"

    # Table schema
    schema = [
        bigquery.SchemaField("transaction_request_id", "STRING"),
        bigquery.SchemaField("charge_id", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("data", "STRING"),
    ]

    # Check and create tables if they don't exist
    def ensure_table_exists(table_ref, schema):
        try:
            bq_client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {table_ref}")

    ensure_table_exists(table_id, schema)
    ensure_table_exists(staging_table_id, schema)

    # Key columns to keep as separate fields
    key_columns = ['transaction_request_id', 'charge_id', 'created_at']

    # Build the transformed dataframe with key columns + JSON data column
    transformed_rows = []
    for _, row in all_data.iterrows():
        row_dict = row.where(pd.notnull(row), None).to_dict()
        
        new_row = {col: row_dict.pop(col, None) for col in key_columns}
        new_row['data'] = json.dumps(row_dict, default=str, ensure_ascii=False)
        transformed_rows.append(new_row)

    transformed_df = pd.DataFrame(transformed_rows)

    # Convert created_at to datetime
    transformed_df['created_at'] = pd.to_datetime(transformed_df['created_at'], errors='coerce', utc=True)

    # Load to staging table (overwrite)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = bq_client.load_table_from_dataframe(transformed_df, staging_table_id, job_config=job_config)
    job.result()

    # MERGE from staging into target table
    merge_query = f"""
    MERGE `{table_id}` AS target
    USING `{staging_table_id}` AS source
    ON target.transaction_request_id = source.transaction_request_id
    WHEN MATCHED THEN
        UPDATE SET
            charge_id = source.charge_id,
            created_at = source.created_at,
            data = source.data
    WHEN NOT MATCHED THEN
        INSERT (transaction_request_id, charge_id, created_at, data)
        VALUES (source.transaction_request_id, source.charge_id, source.created_at, source.data)
    """

    bq_client.query(merge_query).result()

    # Drop staging table
    bq_client.delete_table(staging_table_id)
    logger.info(f"Dropped staging table {staging_table_id}")

    logger.info(f"Merged {len(transformed_df)} rows into BigQuery table {table_id}.")
else:
    logger.warning("No data to write.")
