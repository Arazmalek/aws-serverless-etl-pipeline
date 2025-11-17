import json
import os
import boto3
from botocore.exceptions import ClientError
from typing import Dict, Any

# --- CONFIGURATION (ABSTRACTED FOR PUBLIC PORTFOLIO) ---
# NOTE: In a real system, these would be loaded securely from AWS Secrets Manager.
# Here, we use environment variables for security and portability demonstration.
S3_BUCKET_PLACEHOLDER = os.environ.get("S3_TARGET_BUCKET", "your-etl-landing-zone")
GLUE_WORKFLOW_PLACEHOLDER = os.environ.get("GLUE_WORKFLOW_NAME", "data_ingestion_workflow")
AWS_REGION = os.environ.get("AWS_REGION", "eu-south-1")

def create_presigned_post(bucket_name: str, object_key: str, expiration: int = 300) -> Dict[str, Any]:
    """Generates the presigned URL for a secure, direct file upload to S3."""
    
    s3_client = boto3.client('s3', region_name=AWS_REGION) 

    try:
        response = s3_client.generate_presigned_post(
            Bucket=bucket_name,
            Key=object_key,
            ExpiresIn=expiration
        )
    except ClientError as e:
        # Senior-level error logging would occur here.
        print(f"Error generating presigned post: {e}") 
        return {"error": str(e)}

    return response

def start_generic_glue_workflow(workflow_name: str, client_id: str):
    """
    Triggers the general AWS Glue Workflow, passing the client ID as a parameter.
    This demonstrates the orchestration layer, replacing client-specific 'elif' blocks.
    """
    glue_client = botocto.client('glue', region_name=AWS_REGION)
    try:
        # We pass the client_id to the workflow for dynamic processing (demonstrates routing).
        response = glue_client.start_workflow_run(
            Name=workflow_name,
            Parameters={'--CLIENT_ID': client_id} 
        )
        run_id = response['RunId']
        print(f"Successfully triggered Glue workflow {workflow_name} for client {client_id} with RunId: {run_id}")
        return True
    except ClientError as e:
        print(f"Failed to start Glue workflow {workflow_name}: {e}")
        return False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler. Orchestrates secure ingestion and ETL workflow trigger via API Gateway.
    """
    
    try:
        # 1. Retrieve and Validate Input (CRITICAL: Reading routing parameters from API)
        query_params = event.get('queryStringParameters', {})
        
        # We extract the routing parameters needed for the original logic.
        client_id_uuid = query_params.get('s3', 'CLIENT_ID_MISSING') # Client ID used for routing
        file_name = query_params.get('file_name')
        file_source = query_params.get('gestionale', 'DEFAULT_SOURCE') # Management system source type
        last_file_flag = query_params.get('last_file', 'False')

        # The key path demonstrates the architectural routing (Client ID/Source/Filename).
        key = f'{client_id_uuid}/raw_data/{file_source}/{file_name}' 
        
        if not file_name:
            return {'statusCode': 400, 'body': json.dumps({'message': 'Missing file_name parameter.'})}

        # 2. Generate Presigned URL for Upload
        presigned_response = create_presigned_post(S3_BUCKET_PLACEHOLDER, key)
        
        if "error" in presigned_response:
            return {'statusCode': 500, 'body': json.dumps({'message': 'Failed to generate upload URL.'})}

        # 3. Workflow Trigger Logic
        if last_file_flag == 'True':
            # Trigger the generic workflow, passing the necessary ID for dynamic processing.
            start_generic_glue_workflow(GLUE_WORKFLOW_PLACEHOLDER, client_id_uuid)

        # 4. Success Response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*' 
            },
            'body': json.dumps(presigned_response)
        }

    except Exception as e:
        print(f"Unhandled Lambda Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal Server Error - Check CloudWatch'})
        }
