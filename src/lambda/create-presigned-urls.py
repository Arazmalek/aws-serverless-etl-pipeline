import json
import os
import boto3
from botocore.exceptions import ClientError
from typing import Dict, Any

# --- CONFIGURATION (ABSTRACTED FOR PORTFOLIO) ---
# IMPORTANT: In a production system, these values are loaded securely from AWS Secrets Manager.
# Here, we use placeholder environment variables to show the best practice.
S3_BUCKET_PLACEHOLDER = os.environ.get("S3_TARGET_BUCKET", "your-etl-landing-zone")
GLUE_WORKFLOW_PLACEHOLDER = os.environ.get("GLUE_WORKFLOW_NAME", "data_ingestion_workflow")
AWS_REGION = os.environ.get("AWS_REGION", "eu-south-1") # Example production region

# No need for complex client ID mapping (bucket_dict) in the public repository.

def create_presigned_post(bucket_name: str, object_key: str, expiration: int = 300) -> Dict[str, Any]:
    """
    Generates the presigned URL for a secure direct file upload to S3.
    This demonstrates the secure ingestion layer design.
    """
    s3_client = boto3.client('s3', region_name=AWS_REGION)

    try:
        # Note: Conditions like file size constraints should be included here.
        response = s3_client.generate_presigned_post(
            Bucket=bucket_name,
            Key=object_key,
            ExpiresIn=expiration
        )
    except ClientError as e:
        print(f"Error generating presigned post: {e}")
        return {"error": str(e)}

    return response

def start_glue_workflow(workflow_name: str):
    """
    Triggers the AWS Glue Workflow. This demonstrates the orchestration layer.
    The complex client-specific logic is abstracted.
    """
    glue_client = boto3.client('glue', region_name=AWS_REGION)
    try:
        response = glue_client.start_workflow_run(Name=workflow_name)
        run_id = response['RunId']
        print(f"Successfully triggered Glue workflow {workflow_name} with RunId: {run_id}")
        return True
    except ClientError as e:
        print(f"Failed to start Glue workflow {workflow_name}: {e}")
        return False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler. Orchestrates secure ingestion and ETL workflow trigger.
    """
    try:
        # 1. Input Retrieval (from API Gateway)
        query_params = event.get('queryStringParameters', {})
        file_name = query_params.get('file_name')

        # We abstract the complex client-ID-to-Bucket mapping to a single fixed bucket
        key = f'raw_data/{file_name}' 
        
        if not file_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing file_name parameter.'})
            }

        # 2. Generate Presigned URL for Upload
        presigned_response = create_presigned_post(S3_BUCKET_PLACEHOLDER, key)
        
        if "error" in presigned_response:
            return {'statusCode': 500, 'body': json.dumps({'message': 'Failed to generate upload URL.'})}

        # 3. Workflow Trigger Logic (Simplified for Public Portfolio)
        # We trigger the generic workflow on the 'last file' flag.
        last_file_flag = query_params.get('last_file', 'False')
        if last_file_flag == 'True':
            start_glue_workflow(GLUE_WORKFLOW_PLACEHOLDER)
            
        # 4. Success Response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow_Origin': '*' # Required for API Gateway/CORS
            },
            'body': json.dumps(presigned_response)
        }

    except Exception as e:
        # Senior-level error handling includes detailed logging.
        print(f"Unhandled Lambda Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error - Check CloudWatch'})
        }
