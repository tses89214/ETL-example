"""
This module processes sales data from MinIO, inserts it into a MySQL database,
and then deletes the processed data from MinIO.
"""

import os
import csv
import logging
from io import StringIO

import boto3
from flask import Flask, jsonify
import mysql.connector

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration
MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE')

# MinIO configuration
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_BUCKET_NAME = 'sales-data'


def initialize_minio_client():
    """Initializes and returns a MinIO client."""
    logging.info("Initializing MinIO client")
    try:
        minio_client = boto3.client('s3',
                                    endpoint_url=f"http://{MINIO_ENDPOINT}",
                                    aws_access_key_id=MINIO_ACCESS_KEY,
                                    aws_secret_access_key=MINIO_SECRET_KEY)
        minio_client.head_bucket(Bucket=MINIO_BUCKET_NAME)
        logging.info(
            "MinIO client initialized and bucket '%s' exists", MINIO_BUCKET_NAME)
        return minio_client
    except Exception as exc:
        logging.error("MinIO client initialization failed: %s", exc)
        raise


def initialize_mysql_connection():
    """Initializes and returns a MySQL database connection."""
    logging.info("Initializing MySQL connection")
    try:
        db_connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        logging.info("MySQL connection initialized")
        return db_connection
    except Exception as exc:
        logging.error("MySQL connection initialization failed: %s", exc)
        raise


@app.route('/process_sales_data', methods=['GET'])
def process_sales_data():
    """
    Processes sales data from MinIO, inserts it into a MySQL database,
    and then deletes the processed data from MinIO.
    """
    try:
        minio_client = initialize_minio_client()
        db_connection = initialize_mysql_connection()
    except Exception:
        return jsonify({'message': 'Service unavailable'}), 503

    try:
        # List objects in bucket
        objects = minio_client.list_objects_v2(Bucket=MINIO_BUCKET_NAME)

        if 'Contents' not in objects or not objects['Contents']:
            logging.info("No data found in MinIO bucket")
            return jsonify({'message': 'No sales data to process in MinIO bucket'}), 200

        with db_connection.cursor() as db_cursor:
            for obj in objects['Contents']:
                object_name = obj['Key']
                logging.info("Processing object: %s", object_name)

                # Read data from MinIO
                response = minio_client.get_object(
                    Bucket=MINIO_BUCKET_NAME, Key=object_name)
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

                    sql = """
                    INSERT INTO sales_data (
                    customer_id, product_id, sale_date, quantity, unit_price, total_revenue
                    ) 
                    VALUES (
                        %s, %s, %s, %s, %s, %s
                    )
                    """
                    val = (customer_id, product_id, sale_date,
                           quantity, unit_price, total_revenue)
                    db_cursor.execute(sql, val)
                logging.info("Processed file: %s", object_name)

            db_connection.commit()
            logging.info("Data inserted successfully")

            # Delete object from MinIO
            for obj in objects['Contents']:
                object_name = obj['Key']
                minio_client.delete_object(
                    Bucket=MINIO_BUCKET_NAME, Key=object_name)
                logging.info("Deleted %s from MinIO", object_name)

        return jsonify({'message': 'Sales data processed successfully'}), 200

    except Exception as exc:
        logging.error("Error processing sales data: %s", exc)
        return jsonify({'message': f'Error processing sales data: {str(exc)}'}), 500
    finally:
        if db_connection:
            db_connection.close()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
