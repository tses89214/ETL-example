from flask import Flask, jsonify
import mysql.connector
import boto3
import os
import csv
from io import StringIO

app = Flask(__name__)

# Database configuration
db_host = os.environ.get('MYSQL_HOST')
db_user = os.environ.get('MYSQL_USER')
db_password = os.environ.get('MYSQL_PASSWORD')
db_name = os.environ.get('MYSQL_DATABASE')

# MinIO configuration
minio_endpoint = os.environ.get('MINIO_ENDPOINT')
minio_access_key = os.environ.get('MINIO_ACCESS_KEY')
minio_secret_key = os.environ.get('MINIO_SECRET_KEY')
bucket_name = 'sales-data'

# Initialize MinIO client
print("Initializing MinIO client")
try:
    minio_client = boto3.client('s3',
                                endpoint_url=f"http://{minio_endpoint}",
                                aws_access_key_id=minio_access_key,
                                aws_secret_access_key=minio_secret_key)
    print("MinIO client initialized")

    # Check if bucket exists
    minio_client.head_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' exists")

except Exception as e:
    print(f"MinIO client initialization failed: {e}")

print("Initializing Mysql client")
try:
    # Connect to MySQL database
    db = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )

except Exception as e:
    print(f"Mysql client initialization failed: {e}")


@app.route('/process_sales_data', methods=['GET'])
def process_sales_data():
    # List objects in bucket
    objects = minio_client.list_objects_v2(Bucket=bucket_name)

    if 'Contents' not in objects or not objects['Contents']:
        print("No data found in MinIO bucket")
        return jsonify({'message': 'No sales data to process in MinIO bucket'}), 200

    cursor = db.cursor()

    for obj in objects['Contents']:
        object_name = obj['Key']
        print(f"Processing object: {object_name}")

        # Read data from MinIO
        response = minio_client.get_object(
            Bucket=bucket_name, Key=object_name)
        csv_data = response['Body'].read().decode('utf-8')

        # Parse CSV data
        csv_file = StringIO(csv_data)
        csv_reader = csv.DictReader(csv_file)
        data = list(csv_reader)

        # Insert data into sales_data table
        for item in data:
            customer_id = item['customer_id']
            product_id = item['product_id']
            sale_date = item['sale_date']
            quantity = item['quantity']
            unit_price = item['unit_price']
            total_revenue = item['total_revenue']

            sql = "INSERT INTO sales_data (customer_id, product_id, sale_date, quantity, unit_price, total_revenue) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (customer_id, product_id, sale_date,
                   quantity, unit_price, total_revenue)
            cursor.execute(sql, val)
        print(f"Processed file: {object_name}")

        db.commit()
        print("Data inserted successfully")

        # Delete object from MinIO
        minio_client.delete_object(Bucket=bucket_name, Key=object_name)
        print(f"Deleted {object_name} from MinIO")

    cursor.close()
    return jsonify({'message': 'Sales data processed successfully'}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
