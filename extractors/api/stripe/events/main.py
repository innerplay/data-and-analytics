import stripe
import json
import logging
import datetime
import pandas as pd
import os
import re
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
STRIPE_API_KEY = os.environ["STRIPE_API_KEY"]
DAYS_FROM = int(os.getenv("DAYS_FROM", 1))
DAYS_TO = int(os.getenv("DAYS_TO", 0))

stripe.api_key = STRIPE_API_KEY


def fetch_stripe_events(days_from: int = DAYS_FROM) -> list:

    """
    Fetches Stripe events created after the specified number of days ago.
    
    Args:
        days_from: Number of days ago to start fetching from (default: from env var DAYS_FROM)
                  If days_from=1, fetches events created from yesterday onwards
    
    Returns:
        List of lists containing Stripe subscription objects (pages of results)
    """
    # Calculate the start timestamp based on days_from
    # If days_from=1, we fetch from 1 day ago (yesterday) onwards
    start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_from)
    start_timestamp = int(start_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_timestamp = start_timestamp + 86400
    

    logger.info(f"Fetching Stripe events created after {start_date.date()} (timestamp: {start_timestamp})")
    
    raw_data = []
    seen_ids = set()  # Track subscription IDs to remove duplicates
    starting_after = None
    
    while True:
        events = stripe.Event.list(
            types=['customer.subscription.updated','customer.updated','charge.updated'],
            created=dict[str, int](
                gte=start_timestamp,
                lt=end_timestamp
            ),
            limit=100,
            starting_after=starting_after
        )
        
        payload = events.data
        if payload:
            # Filter out duplicates based on subscription ID
            unique_payload = []
            for event in payload:
                # Extract ID from subscription object
                event_id = None
                if hasattr(event, 'id'):
                    event_id = event.id
                elif isinstance(event, dict) and 'id' in event:
                    event_id = event['id']
                
                if event_id and event_id not in seen_ids:
                    seen_ids.add(event_id)
                    unique_payload.append(event)
                elif event_id:
                    logger.debug(f"Skipping duplicate event: {event_id}")
            
            if unique_payload:
                raw_data.append(unique_payload)
                logger.info(f"Fetched page with {len(unique_payload)} unique events (skipped {len(payload) - len(unique_payload)} duplicates)")
        
        if events.get('has_more'):
            starting_after = events.data[-1].get('id')
        else:
            break
    
    total_events = sum(len(page) for page in raw_data)
    logger.info(f"Total unique events fetched: {total_events} across {len(raw_data)} pages")

    return raw_data

def normalize_key(key: str) -> str:
    """
    Normalizes key names to snake_case and replaces dots with underscores.
    """
    # Replace dots with underscores for hierarchical keys
    key = key.replace('.', '_')
    # Replace hyphens with underscores
    key = key.replace('-', '_')
    # Convert camelCase to snake_case
    key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
    # Collapse multiple underscores
    key = re.sub(r'__+', '_', key)
    return key

def transform_to_bigquery_table(raw_data: list) -> pd.DataFrame:
    """
    Transforms raw Stripe subscription data into a simplified format with 3 columns:
    - id: subscription ID (string)
    - data: full subscription data as JSON string
    - etl_load_date: timestamp of when data was loaded
    
    Args:
        raw_data: List of lists containing Stripe subscription objects (pages of results)
    
    Returns:
        pandas.DataFrame: DataFrame with columns: id, data, etl_load_date
    """
    logger.info("Starting transformation of raw data to BigQuery format")
    
    # Flatten the nested list structure (pages) into a single list
    all_events = []
    for page in raw_data:
        if page:
            all_events.extend(page)
    
    logger.info(f"Processing {len(all_events)} events")
    
    # Transform to simplified format
    transformed_rows = []
    for event in all_events:
        # Convert Stripe objects to dictionaries if needed
        if hasattr(event, '__dict__'):
            event_dict = dict(event)
        elif isinstance(event, dict):
            event_dict = event
        else:
            logger.warning(f"Skipping event with unexpected type: {type(event)}")
            continue
        
        # Extract ID
        event_id = event_dict.get('id')
        if not event_id:
            logger.warning("Skipping event without ID")
            continue

        # Extract event timestamp
        event_timestamp = event_dict.get('created')
        if not event_timestamp:
            logger.warning("Skipping event without event_timestamp")
            continue

        # extract updated object id
        object_id = event_dict.get('data', {}).get('object', {}).get('id')
        if not object_id:
            logger.warning("Skipping event without updated object ID")
            continue

        data_json = json.dumps(event_dict.get('data', {}).get('object', {}), default=str, ensure_ascii=False)
        if not data_json:
            logger.warning("Skipping event without updated object ID")
            continue
        
        # Add ETL load date
        etl_load_date = datetime.datetime.now(datetime.timezone.utc)
        
        transformed_rows.append({
            'id': event_id,
            'event_timestamp': event_timestamp,
            'object_id': object_id,
            'data': data_json,
            'etl_load_date': etl_load_date
        })
    
    # Convert to DataFrame
    if transformed_rows:
        df = pd.DataFrame(transformed_rows)
        logger.info(f"Created DataFrame with {len(df)} rows and 5 columns: id, event_timestamp, object_id, data, etl_load_date")
        return df
    else:
        logger.warning("No data to transform")
        return pd.DataFrame(columns=['id', 'event_timestamp', 'object_id', 'data', 'etl_load_date'])


def merge_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    table_name: str,
    primary_key: str = "id"
) -> None:
    """
    Connects to BigQuery and merges DataFrame rows into a table using a staging table approach.
    
    Args:
        df: DataFrame to merge into BigQuery
        project_id: GCP project ID
        dataset: BigQuery dataset name
        table_name: Target table name (without project/dataset prefix)
        primary_key: Column name to use as primary key for merge (default: "id")
        credentials_json: Optional JSON string with service account credentials.
                         If None, uses default credentials or GOOGLE_APPLICATION_CREDENTIALS_JSON env var
    
    Raises:
        ValueError: If DataFrame is empty or primary_key column is missing
        Exception: For BigQuery connection or operation errors
    """
    if df.empty:
        logger.warning("DataFrame is empty. Nothing to merge.")
        return
    
    if primary_key not in df.columns:
        raise ValueError(f"Primary key column '{primary_key}' not found in DataFrame columns: {list(df.columns)}")
    
    logger.info(f"Starting BigQuery merge for {len(df)} rows into {project_id}.{dataset}.{table_name}")
    
    # Initialize BigQuery client
    credentials = service_account.Credentials.from_service_account_info(json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON']))
    bq_project_id = credentials.project_id
    
    bq_client = bigquery.Client(credentials=credentials, project=bq_project_id)
    
    # Table IDs
    table_id = f"{bq_project_id}.{dataset}.{table_name}"
    staging_table_id = f"{bq_project_id}.staging.{table_name}"
    
    # Define schema for the 3 columns
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_timestamp", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("object_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("data", "STRING", mode="REQUIRED"),  # JSON stored as STRING
        bigquery.SchemaField("etl_load_date", "TIMESTAMP", mode="NULLABLE")
    ]
    
    # Ensure etl_load_date is present and is a timestamp
    if 'etl_load_date' not in df.columns:
        df = df.copy()
        df['etl_load_date'] = datetime.datetime.now(datetime.timezone.utc)
    
    # Ensure etl_load_date is datetime type
    if 'etl_load_date' in df.columns:
        df = df.copy()
        df['etl_load_date'] = pd.to_datetime(df['etl_load_date'], errors='coerce', utc=True)
    
    # Ensure we only have the 3 required columns in the correct order
    required_columns = ['id', 'event_timestamp', 'object_id', 'data', 'etl_load_date']
    if list(df.columns) != required_columns:
        # Reorder and filter to only required columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        df = df[required_columns].copy()
        logger.info(f"Reordered DataFrame columns to: {required_columns}")
    
    # Ensure tables exist
    def ensure_table_exists(table_ref: str):
        try:
            bq_client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            bq_client.create_table(table)
            logger.info(f"Created table {table_ref}")
    
    ensure_table_exists(table_id)
    ensure_table_exists(staging_table_id)
    
    # Load to staging table (overwrite)
    logger.info(f"Loading {len(df)} rows to staging table {staging_table_id}")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    
    job = bq_client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    job.result()  # Wait for completion
    logger.info(f"Successfully loaded {len(df)} rows to staging table")
    
    # Build MERGE query
    # Get all columns except primary_key for UPDATE SET
    update_columns = [col for col in df.columns if col != primary_key]
    update_set_clause = ",\n            ".join([f"{col} = source.{col}" for col in update_columns])
    insert_columns = ", ".join(df.columns)
    insert_values = ", ".join([f"source.{col}" for col in df.columns])
    
    merge_query = f"""
    MERGE `{table_id}` AS target
    USING `{staging_table_id}` AS source
    ON target.{primary_key} = source.{primary_key}
    WHEN MATCHED THEN
        UPDATE SET
            {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    
    logger.info(f"Executing MERGE query to merge staging into {table_id}")
    merge_job = bq_client.query(merge_query)
    merge_job.result()  # Wait for completion
    logger.info(f"Successfully merged data into {table_id}")
    
    # Drop staging table
    logger.info(f"Dropping staging table {staging_table_id}")
    bq_client.delete_table(staging_table_id, not_found_ok=True)
    logger.info(f"Dropped staging table {staging_table_id}")
    
    logger.info(f"Completed merge: {len(df)} rows processed into {table_id}")


if __name__ == "__main__":
    # Fetch events from Stripe
    for i in range(DAYS_FROM, DAYS_TO - 1, -1):
        raw_data = fetch_stripe_events(i)
        
        # Transform to BigQuery format
        df_bigquery = transform_to_bigquery_table(raw_data)
        
        if not df_bigquery.empty:
            # Merge to BigQuery
            merge_to_bigquery(
                df=df_bigquery,
                project_id="analytics-473217",
                dataset="raw_data",
                table_name="stripe_events",
                primary_key="id"
            )