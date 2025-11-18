import sys
import boto3
import msal
import os
import requests
import logging
from datetime import datetime
from awsglue.utils import getResolvedOptions

# --- 1. SETUP LOGGING & CONFIGURATION ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Retrieve secure parameters from Glue Job Arguments / Environment Variables
# IMPORTANT: Never hardcode credentials (even base64) in the script.
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_TARGET_BUCKET',    # The bucket where clean data resides
    'CLIENT_ID',           # Used for routing/folder structure
    'ONEDRIVE_PATH',       # Target path inside OneDrive
    # Secrets should be passed as --arguments or fetched from Secrets Manager
    'MS_TENANT_ID',
    'MS_CLIENT_ID',
    'MS_USERNAME',
    'MS_PASSWORD' 
])

S3_BUCKET_NAME = args['S3_TARGET_BUCKET']
TARGET_CLIENT_ID = args['CLIENT_ID']
ONEDRIVE_ROOT_PATH = args['ONEDRIVE_PATH']

# Standardized S3 Paths
CLEAN_PREFIX = f"clean_data/{TARGET_CLIENT_ID.lower()}"
HISTORY_PREFIX = f"historical_clean_data/{TARGET_CLIENT_ID.lower()}"

s3_bucket = boto3.resource("s3").Bucket(S3_BUCKET_NAME)
s3_client = boto3.client("s3")

# --- 2. AUTHENTICATION CLASS (Microsoft Graph API) ---
class OneDriveConnector:
    def __init__(self):
        self.authority_url = f'https://login.microsoftonline.com/{args["MS_TENANT_ID"]}'
        self.resource_url = 'https://graph.microsoft.com/'
        self.api_version = 'v1.0'
        self.scopes = ['Sites.ReadWrite.All', 'Files.ReadWrite.All']
        
    def get_authenticated_headers(self):
        """Acquires token via MSAL and returns headers."""
        try:
            app = msal.PublicClientApplication(
                args['MS_CLIENT_ID'], 
                authority=self.authority_url
            )
            # Using ROPC flow (Username/Password) - suitable for background service accounts
            token_response = app.acquire_token_by_username_password(
                args['MS_USERNAME'], 
                args['MS_PASSWORD'], 
                scopes=self.scopes
            )
            
            if 'access_token' in token_response:
                logger.info("Successfully authenticated with Microsoft Graph API.")
                return {'Authorization': 'Bearer ' + token_response['access_token']}
            else:
                logger.error(f"Token acquisition failed: {token_response.get('error_description')}")
                raise Exception("Authentication Failed")
                
        except Exception as e:
            logger.error(f"MSAL Error: {e}")
            raise e

# --- 3. CORE LOGIC: UPLOAD & ARCHIVE ---

def process_clean_files():
    """
    1. Reads clean files from S3.
    2. Uploads to OneDrive.
    3. Archives file to Historical folder in S3.
    """
    logger.info(f"--- Starting OneDrive Export for Client: {TARGET_CLIENT_ID} ---")
    
    # Initialize Auth
    connector = OneDriveConnector()
    headers = connector.get_authenticated_headers()
    
    onedrive_base_url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{ONEDRIVE_ROOT_PATH}"
    timestamp = datetime.now().strftime("%Y_%m_%d_%H")

    # List files in Clean Data folder
    files = list(s3_bucket.objects.filter(Prefix=CLEAN_PREFIX))
    
    if not files:
        logger.warning(f"No clean files found in path: {CLEAN_PREFIX}")
        return

    for obj in files:
        # Skip folder markers
        if obj.key.endswith('/'):
            continue
            
        file_full_key = obj.key
        file_name = os.path.basename(file_full_key)
        
        logger.info(f"Processing file: {file_name}")

        try:
            # Step A: Read from S3
            file_content = obj.get()['Body'].read()
            
            # Step B: Upload to OneDrive
            # PUT /me/drive/root:/{path}/{filename}:/content
            upload_url = f"{onedrive_base_url}/{file_name}:/content"
            
            response = requests.put(upload_url, data=file_content, headers=headers)
            
            if response.status_code in [200, 201]:
                logger.info(f"UPLOAD SUCCESS: {file_name} uploaded to OneDrive.")
                
                # Step C: Archive to History (Backup)
                history_key = f"{HISTORY_PREFIX}/{timestamp}/{file_name}"
                copy_source = {'Bucket': S3_BUCKET_NAME, 'Key': file_full_key}
                
                s3_bucket.copy(copy_source, history_key)
                logger.info(f"ARCHIVE SUCCESS: Copied to {history_key}")
                
            else:
                logger.error(f"UPLOAD FAILED: {file_name} - Status: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"CRITICAL ERROR processing {file_name}: {e}")

    logger.info("--- OneDrive Export Process Completed ---")

if __name__ == "__main__":
    process_clean_files()
