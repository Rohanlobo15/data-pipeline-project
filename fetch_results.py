import boto3

s3 = boto3.client('s3', region_name='us-east-1')

response = s3.list_objects_v2(
    Bucket='rohan-data-pipeline-2024',
    Prefix='raw/'
)

print('All files in raw/:')
for obj in response['Contents']:
    print(f"  {obj['Key']}  ({obj['Size']} bytes)")