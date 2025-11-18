import sys
import boto3
import logging
import os
from datetime import datetime
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

# --- 1. SETUP LOGGING & CONFIGURATION ---
# Setting up professional logging instead of print statements
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Retrieve arguments passed from the Glue Workflow or Job definition
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_BUCKET', 'CLIENT_ID'])

S3_BUCKET_NAME = args['S3_TARGET_BUCKET']
CLIENT_ID = args['CLIENT_ID']

# Standardized Paths (Routing Logic)
# We use the CLIENT_ID to dynamically locate the specific client's data
RAW_PREFIX = f"{CLIENT_ID}/raw_data"
CATALOG_PREFIX = f"{CLIENT_ID}/data_catalog"
HISTORY_PREFIX = f"{CLIENT_ID}/historical_raw_data"

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_timestamp_str():
    """Generates a standard timestamp for archival folders."""
    return datetime.now().strftime("%Y_%m_%d_%H")

def copy_s3_object(source_bucket, source_key, dest_bucket, dest_key):
    """Helper function to securely copy S3 objects with error handling."""
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    try:
        s3_resource.meta.client.copy(copy_source, dest_bucket, dest_key)
        logger.info(f"SUCCESS: Copied to {dest_key}")
    except ClientError as e:
        logger.error(f"ERROR: Failed to copy {source_key} to {dest_key}: {e}")
        raise e

# --- 2. CORE LOGIC: ARCHIVAL & CATALOG PREPARATION ---

def archive_and_organize_raw_data():
    """
    Iterates through the Raw Landing Zone.
    1. Archives data to 'Historical' folder (Audit Trail).
    2. Moves data to 'Catalog' folder (partitioned by table) for the Glue Crawler.
    """
    logger.info(f"--- Starting Raw Data Management for Client: {CLIENT_ID} ---")
    
    bucket = s3_resource.Bucket(S3_BUCKET_NAME)
    timestamp = get_timestamp_str()
    
    # List objects in the Raw Data prefix
    # Note: In a high-volume production scenario, we would use Paginators here.
    raw_objects = list(bucket.objects.filter(Prefix=RAW_PREFIX))
    
    if not raw_objects:
        logger.warning(f"No files found in raw path: {RAW_PREFIX}")
        return

    for obj in raw_objects:
        # Skip folder markers (keys ending in /)
        if obj.key.endswith('/'):
            continue
            
        file_full_key = obj.key
        # Extract just the filename (e.g., 'raw_data/sales.csv' -> 'sales.csv')
        file_name = os.path.basename(file_full_key)
        
        # Logic to derive table name from filename (e.g., 'sales.csv' -> 'sales')
        # Improved robust split using os.path.splitext to handle any extension safely
        table_name = os.path.splitext(file_name)[0]
        
        # --- ACTION A: HISTORICAL ARCHIVE (Backup) ---
        # Path: client/historical_raw_data/YYYY_MM_DD_HH/filename.csv
        history_key = f"{HISTORY_PREFIX}/{timestamp}/{file_name}"
        copy_s3_object(S3_BUCKET_NAME, file_full_key, S3_BUCKET_NAME, history_key)
        
        # --- ACTION B: CATALOG PREPARATION (For Crawler) ---
        # Path: client/data_catalog/table_name/filename.csv
        # This structure allows the Crawler to recognize 'table_name' as a table.
        catalog_key = f"{CATALOG_PREFIX}/{table_name}/{file_name}"
        copy_s3_object(S3_BUCKET_NAME, file_full_key, S3_BUCKET_NAME, catalog_key)

    logger.info("--- Raw Data Management Completed Successfully ---")

if __name__ == "__main__":
    archive_and_organize_raw_data()
