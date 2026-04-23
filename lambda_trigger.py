import boto3
import json
import urllib.request
import subprocess
import sys

# Install requests if not available
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests'])
import requests

DATABRICKS_URL = "https://dbc-89d5978b-0652.cloud.databricks.com"
NOTEBOOK_PATH = "/Workspace/Users/rohanlobo15@gmail.com/data-pipeline-project/pipeline_transform"
DATABRICKS_JOB_ID = "1000463815687200"
BUCKET = "rohan-data-pipeline-2024"

def lambda_handler(event, context):
    s3_event = event['Records'][0]['s3']
    bucket = s3_event['bucket']['name']
    key = s3_event['object']['key']
    
    print(f"New file detected: s3://{bucket}/{key}")
    
    # Only trigger for raw CSV files
    if not key.startswith('raw/') or not key.endswith('.csv'):
        print("Not a raw CSV, skipping")
        return {'statusCode': 200, 'body': 'Skipped'}
    
    print(f"Raw CSV detected! Triggering Databricks pipeline for: {key}")
    
    # Get Databricks token from AWS Secrets Manager
    secrets = boto3.client('secretsmanager', region_name='us-east-1')
    secret = secrets.get_secret_value(SecretId='databricks-token')
    databricks_token = secret['SecretString']
    
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    
    # Step 1: Trigger Databricks notebook run
    print("Step 1: Triggering Databricks notebook...")
    run_payload = {
        "run_name": f"pipeline-{key.split('/')[-1]}",
        "existing_cluster_id": None,
        "notebook_task": {
            "notebook_path": NOTEBOOK_PATH,
            "base_parameters": {
                "input_file": key,
                "bucket": bucket
            }
        },
        "new_cluster": {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
            "num_workers": 1
        }
    }
    
    response = requests.post(
        f"{DATABRICKS_URL}/api/2.1/jobs/runs/submit",
        headers=headers,
        json=run_payload
    )
    
    if response.status_code == 200:
        run_id = response.json().get('run_id')
        print(f"Databricks job started! Run ID: {run_id}")
    else:
        print(f"Databricks trigger failed: {response.text}")
    
    # Log the trigger
    s3 = boto3.client('s3')
    log_entry = {
        "triggered_by": key,
        "bucket": bucket,
        "databricks_run_id": run_id if response.status_code == 200 else "failed",
        "status": "pipeline_triggered"
    }
    
    s3.put_object(
        Bucket=BUCKET,
        Key=f"logs/triggers/{key.split('/')[-1]}.log",
        Body=json.dumps(log_entry)
    )
    
    return {'statusCode': 200, 'body': f'Pipeline triggered, run_id: {run_id}'}