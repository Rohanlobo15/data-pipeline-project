import boto3
import json

def lambda_handler(event, context):
    # This function runs automatically when a new file lands in S3
    
    s3_event = event['Records'][0]['s3']
    bucket = s3_event['bucket']['name']
    key = s3_event['object']['key']
    
    print(f"New file detected: s3://{bucket}/{key}")
    
    # Only trigger for files in raw/ folder
    if not key.startswith('raw/'):
        print("Not a raw file, skipping")
        return {'statusCode': 200, 'body': 'Skipped'}
    
    # Only trigger for CSV files
    if not key.endswith('.csv'):
        print("Not a CSV file, skipping")
        return {'statusCode': 200, 'body': 'Skipped'}
    
    print(f"Raw CSV detected! Triggering pipeline for: {key}")
    
    # Log the trigger to S3 for monitoring
    s3 = boto3.client('s3')
    log_entry = {
        "triggered_by": key,
        "bucket": bucket,
        "status": "pipeline_triggered"
    }
    
    s3.put_object(
        Bucket=bucket,
        Key=f"logs/triggers/{key.split('/')[-1]}.log",
        Body=json.dumps(log_entry)
    )
    
    print("Pipeline triggered successfully!")
    return {'statusCode': 200, 'body': 'Pipeline triggered'}