import boto3

glue = boto3.client('glue', region_name='us-east-1')

# Step 1: Create Glue Database
try:
    glue.create_database(
        DatabaseInput={
            'Name': 'pipeline_db',
            'Description': 'Our ecommerce pipeline database'
        }
    )
    print('Database created successfully')
except glue.exceptions.AlreadyExistsException:
    print('Database already exists, moving on')

# Step 2: Create Crawler
try:
    glue.create_crawler(
        Name='pipeline-crawler',
        Role='GlueS3Role',
        DatabaseName='pipeline_db',
        Targets={
            'S3Targets': [
                {'Path': 's3://rohan-data-pipeline-2024/raw/'}
            ]
        }
    )
    print('Crawler created successfully')
except glue.exceptions.AlreadyExistsException:
    print('Crawler already exists, moving on')

# Step 3: Start Crawler
glue.start_crawler(Name='pipeline-crawler')
print('Crawler started, scanning your S3 files...')
print('Wait 30 seconds then we will check if it finished')