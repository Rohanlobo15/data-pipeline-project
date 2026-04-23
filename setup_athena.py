import boto3

athena = boto3.client('athena', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

# Create a folder in your bucket for Athena results
s3.put_object(
    Bucket='rohan-data-pipeline-2024',
    Key='athena-results/'
)
print('Athena results folder created')

# Run a test query - count all orders
response = athena.start_query_execution(
    QueryString='SELECT COUNT(*) as total_orders FROM pipeline_db.orders_20260423_154330',
    ResultConfiguration={
        'OutputLocation': 's3://rohan-data-pipeline-2024/athena-results/'
    }
)

query_id = response['QueryExecutionId']
print(f'Query started, ID: {query_id}')
print('Wait 10 seconds then we will fetch the results')