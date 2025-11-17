import requests
import os
from os.path import join, isfile, expanduser
import pandas as pd
import pyodbc
from typing import List, Dict, Tuple, Callable
from botocore.exceptions import ClientError

# --- CONFIGURATION (SECURITY AND ABSTRACTION) ---
DB_HOST = os.environ.get("DB_HOST", "DB_HOST_PLACEHOLDER")
DB_PORT = os.environ.get("DB_PORT", "1433")
DB_NAME = os.environ.get("DB_NAME", "SAMPLE_ERP_DB")
DB_USER = os.environ.get("DB_USER", "db_user_placeholder")
DB_PASS = os.environ.get("DB_PASS", "secure_password_placeholder")
API_GATEWAY_ENDPOINT = os.environ.get("API_GATEWAY_ENDPOINT", "https://api.example.com/v1/presigned-urls")
API_KEY = os.environ.get("API_KEY", "SECURE_API_KEY_HERE") # API Key for X-API-KEY header
CLIENT_ID = os.environ.get("CLIENT_ID", "4097bdae-065c-4a1f-bcce-b0b4cdde7e09_SAMPLE")
GESTIONALE_NAME = os.environ.get("GESTIONALE_NAME", "Zucchetti_System")

# --- LOCAL FILE SETUP ---
try:
    HOME_DIR = expanduser('~')
    # Path is set to a dedicated project folder for cleanup and safety
    FOLDER_PATH = join(HOME_DIR, 'git_etl_temp', 'extracted_files') 
except Exception as e:
    print(f"Error finding temporary path: {e}") 


# --- CORE ETL LOGIC (EXTRACT & TRANSFORM) ---

def _execute_and_save(conn: pyodbc.Connection, folder_path: str, table_name: str, query: str):
    """Executes SQL query, reads to DataFrame, and saves as CSV."""
    
    print(f"--- Processing: {table_name}")
    try:
        # Using read_sql to demonstrate Pandas' ability to handle large queries
        df = pd.read_sql(query, conn)
        file_path = join(folder_path, f"{table_name}.csv")
        df.to_csv(file_path, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"    File {table_name}.csv with {len(df)} rows saved successfully.")
        
    except pd.errors.DatabaseError as e:
        # Specific error handling based on operation
        print(f"    Error executing query for {table_name}: {e}")
    except Exception as e:
        print(f"    Error saving file {table_name}: {e}")


def get_extraction_functions() -> List[Tuple[str, Callable]]:
    """Returns a list of extraction functions and their table names for cleaner main loop."""
    # We simplify and anonymize the client's original table names for security and portfolio clarity.
    
    return [
        # Original complex tables anonymized to general data types for clarity
        ("customer_masters", lambda conn, path: _execute_and_save(conn, path, "customer_masters", "SELECT * FROM CustomerMasterData_Sample")),
        ("finance_accounts", lambda conn, path: _execute_and_save(conn, path, "finance_accounts", "SELECT * FROM GL_Accounts")),
        ("transaction_details", lambda conn, path: _execute_and_save(conn, path, "transaction_details", "SELECT * FROM TransactionDetails_Sample")),
        # We replace the complex original join logic with a simplified structure for demonstration.
        ("complex_join_demo", lambda conn, path: _execute_and_save(conn, path, "complex_join_demo", 
                                                                    "SELECT * FROM ComplexTable a LEFT JOIN SimpleTable b ON a.ID=b.ID"))
    ]


def extract_all_data(extraction_functions: List[Tuple[str, Callable]]):
    """
    Main function that connects to the DB and orchestrates all data extraction.
    """
    print(f"--- 1. Starting data extraction process ---")
    
    if not os.path.exists(FOLDER_PATH):
        os.makedirs(FOLDER_PATH)
        print(f"Folder {FOLDER_PATH} created.")

    # Connection string uses environment variables for security
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_HOST},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        f"TrustServerCertificate=yes;"
    )

    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        print("Successfully connected to SQL Server database.")
        
        # Execute all defined extractions
        for name, func in extraction_functions:
            func(conn, FOLDER_PATH)

    except pyodbc.Error as ex:
        # Detailed error handling for specific SQL states (Professional Practice)
        sqlstate = ex.args[0]
        if sqlstate == '08001':
            print("Connection Error: ODBC Driver not found or server is unavailable.")
        elif sqlstate == '28000':
            print("Connection Error: Incorrect username or password.")
        else:
            print(f"Database Error: {ex}")
            
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
    
    print("--- 1. End of extraction process ---")



def send_files_to_api():

    print(f"\n--- 2. Starting file upload process ---")
    
    try:
        files_to_upload = [f for f in listdir(FOLDER_PATH) if isfile(join(FOLDER_PATH, f))]
        total_files = len(files_to_upload)
    except FileNotFoundError:
        print(f"Error: Folder {FOLDER_PATH} not found. Please run extraction first.")
        return
    
    if total_files == 0:
        print("No files found in the folder to upload.")
        return

    for i, f in enumerate(files_to_upload):
        
        current_file_count = total_files - i
        # last_file flag triggers the Glue workflow on the final file
        last_file = 'True' if current_file_count == 1 else 'False'
        
        print(f"\nProcessing file {i+1}/{total_files}: {f} (LastFile: {last_file})")
        
        # Build the API Gateway URL using abstracted parameters
        # This demonstrates architectural routing (Client ID/Source/Filename).
        url = (
            f"{API_GATEWAY_ENDPOINT}?"
            f"s3={CLIENT_ID}&" # Passes the Client ID for routing
            f"file_name={f}&"
            f"gestionale={GESTIONALE_NAME}/raw_data&"
            f"last_file={last_file}"
        )
        
        headers = {'x-api-key': API_KEY} # API Key used for security layer
        
        try:
            # 1. Get Presigned URL from AWS API Gateway/Lambda
            ps_url_resp = requests.get(url, headers=headers)
            print(f"Presigned URL request status: {ps_url_resp.status_code}")

            if ps_url_resp.status_code == 200:
                json_resp = ps_url_resp.json()
                file_path = join(FOLDER_PATH, f)
                
                # 2. Upload file directly to S3 using the received fields/URL
                with open(file_path, 'rb') as file_content:
                    files_for_upload = {'file': (file_path, file_content.read())}
                
                upload_file_resp = requests.post(json_resp['url'], data=json_resp['fields'], files=files_for_upload)
                
                print(f"Upload file status code: {upload_file_resp.status_code}")

                if upload_file_resp.status_code == 204:
                    os.remove(file_path)
                    print(f"File {file_path} was successfully uploaded and deleted.")
                else:
                    print(f"Error uploading file: {upload_file_resp.text}")
            
            else:
                 print(f"Error getting Presigned URL (Status {ps_url_resp.status_code}): {ps_url_resp.text}")
        
        except Exception as e:
            print(f"Critical error processing file {f}: {e}")

    print('--- 2. End of upload process ---')
    print('DONE')

if __name__ == "__main__":
    # Get the defined extraction functions
    extraction_plan = get_extraction_functions()
    # Execute the extraction process
    extract_all_data(extraction_plan)
    # Execute the upload process
    send_files_to_api()
