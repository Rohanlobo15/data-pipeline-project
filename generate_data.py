import json
import random
import boto3
from faker import Faker
from datetime import datetime

fake = Faker()

# Connect to S3
s3 = boto3.client('s3')
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

def upload_to_s3(data, filename):
    content = '\n'.join([json.dumps(record) for record in data])
    key = f'raw/{filename}'
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=content,
        ContentType='application/json'
    )
    print(f'Uploaded {len(data)} records to s3://{BUCKET}/{key}')

if __name__ == '__main__':
    print('Generating data...')
    
    customers = generate_customers(100)
    products = generate_products(50)
    orders = generate_orders(500)
    
    print('Uploading to S3...')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    upload_to_s3(customers, f'customers_{timestamp}.json')
    upload_to_s3(products, f'products_{timestamp}.json')
    upload_to_s3(orders, f'orders_{timestamp}.json')
    
    print('Done! All data is in your S3 raw layer.')