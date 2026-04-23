import json
import random
import boto3
import csv
import io
from faker import Faker
from datetime import datetime

fake = Faker()
s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'rohan-data-pipeline-2024'

def generate_customers(n=100):
    customers = []
    for i in range(1, n+1):
        customers.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "country": fake.country(),
            "signup_date": fake.date_between(start_date='-2y', end_date='today').isoformat()
        })
    return customers

def generate_products(n=50):
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    products = []
    for i in range(1, n+1):
        products.append({
            "product_id": i,
            "name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "category": random.choice(categories),
            "price": round(random.uniform(5.0, 500.0), 2),
            "stock": random.randint(0, 1000)
        })
    return products

def generate_orders(n=500, customer_count=100, product_count=50):
    statuses = ['completed', 'pending', 'cancelled', 'refunded']
    orders = []
    for i in range(1, n+1):
        orders.append({
            "order_id": i,
            "customer_id": random.randint(1, customer_count),
            "product_id": random.randint(1, product_count),
            "quantity": random.randint(1, 10),
            "status": random.choice(statuses),
            "order_date": fake.date_between(start_date='-1y', end_date='today').isoformat(),
            "total_amount": round(random.uniform(10.0, 5000.0), 2)
        })
    return orders

def upload_csv(data, filename):
    # Convert to CSV - Athena reads this perfectly
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    
    key = f'raw/{filename}'
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType='text/csv'
    )
    print(f'Uploaded {len(data)} records to s3://{BUCKET}/{key}')

if __name__ == '__main__':
    print('Generating fresh data as CSV...')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    upload_csv(generate_customers(100), f'customers_{timestamp}.csv')
    upload_csv(generate_products(50), f'products_{timestamp}.csv')
    upload_csv(generate_orders(500), f'orders_{timestamp}.csv')
    
    print('Done! CSV files uploaded to S3 raw layer.')
    print('Now we need to re-run the Glue crawler to pick up the new files.')