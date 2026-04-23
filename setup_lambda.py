import boto3
import json
import zipfile
import os

# First create the IAM role for Lambda
iam = boto3.client('iam', region_name='us-east-1')
lambda_client = boto3.client('lambda', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

BUCKET = 'rohan-data-pipeline-2024'

# Step 1: Create IAM role for Lambda
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    ]
}

try:
    role = iam.create_role(
        RoleName='LambdaPipelineRole',
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description='Role for pipeline Lambda trigger'
    )
    role_arn = role['Role']['Arn']
    print(f"Role created: {role_arn}")
except iam.exceptions.EntityAlreadyExistsException:
    role_arn = iam.get_role(RoleName='LambdaPipelineRole')['Role']['Arn']
    print(f"Role already exists: {role_arn}")

# Attach policies to the role
policies = [
    'arn:aws:iam::aws:policy/AmazonS3FullAccess',
    'arn:aws:iam::aws:policy/CloudWatchFullAccess'
]

for policy in policies:
    try:
        iam.attach_role_policy(RoleName='LambdaPipelineRole', PolicyArn=policy)
        print(f"Attached: {policy.split('/')[-1]}")
    except:
        print(f"Policy already attached: {policy.split('/')[-1]}")

# Wait for role to propagate
import time
print("Waiting for role to propagate...")
time.sleep(10)

# Step 2: Zip the Lambda function
with zipfile.ZipFile('lambda_trigger.zip', 'w') as z:
    z.write('lambda_trigger.py')
print("Lambda function zipped")

# Step 3: Create Lambda function
with open('lambda_trigger.zip', 'rb') as f:
    zip_content = f.read()

try:
    response = lambda_client.create_function(
        FunctionName='pipeline-trigger',
        Runtime='python3.10',
        Role=role_arn,
        Handler='lambda_trigger.lambda_handler',
        Code={'ZipFile': zip_content},
        Timeout=30,
        Description='Triggers pipeline when new CSV lands in S3 raw folder'
    )
    print(f"Lambda created: {response['FunctionArn']}")
    lambda_arn = response['FunctionArn']
except lambda_client.exceptions.ResourceConflictException:
    response = lambda_client.get_function(FunctionName='pipeline-trigger')
    lambda_arn = response['Configuration']['FunctionArn']
    print(f"Lambda already exists: {lambda_arn}")

# Step 4: Give S3 permission to invoke Lambda
try:
    lambda_client.add_permission(
        FunctionName='pipeline-trigger',
        StatementId='S3InvokePermission',
        Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',
        SourceArn=f'arn:aws:s3:::{BUCKET}'
    )
    print("S3 invoke permission added")
except lambda_client.exceptions.ResourceConflictException:
    print("Permission already exists")

# Step 5: Add S3 trigger - notify Lambda when new file lands in raw/
notification_config = {
    'LambdaFunctionConfigurations': [
        {
            'LambdaFunctionArn': lambda_arn,
            'Events': ['s3:ObjectCreated:*'],
            'Filter': {
                'Key': {
                    'FilterRules': [
                        {'Name': 'prefix', 'Value': 'raw/'},
                        {'Name': 'suffix', 'Value': '.csv'}
                    ]
                }
            }
        }
    ]
}

s3.put_bucket_notification_configuration(
    Bucket=BUCKET,
    NotificationConfiguration=notification_config
)
print(f"S3 trigger configured!")
print(f"Now whenever a CSV lands in s3://{BUCKET}/raw/ this Lambda will fire automatically!")