import boto3

s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'rohan-data-pipeline-2024'

files = [
    ('raw/orders_20260423_161016.csv', 'raw/orders/orders_20260423_161016.csv'),
    ('raw/customers_20260423_161016.csv', 'raw/customers/customers_20260423_161016.csv'),
    ('raw/products_20260423_161016.csv', 'raw/products/products_20260423_161016.csv'),
]

for source, destination in files:
    # Copy to new location
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={'Bucket': BUCKET, 'Key': source},
        Key=destination
    )
    print(f'Copied to {destination}')

print('Done! Files reorganized into subfolders.')