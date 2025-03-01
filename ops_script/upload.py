"""
This script uploads a file to a MinIO bucket.
"""

import os
import logging
import boto3

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# MinIO configuration
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
BUCKET_NAME = 'sales-data'
OBJECT_NAME = 'sales_data.csv'
FILE_PATH = 'test_data/sales_data.csv'.replace('/', '\\')


def upload_to_minio(file_path, bucket_name, object_name):
    """
    Uploads a file to a MinIO bucket.

    Args:
        file_path (str): The path to the file to upload.
        bucket_name (str): The name of the bucket to upload to.
        object_name (str): The name of the object to create in the bucket.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    try:
        # Create MinIO client
        minio_client = boto3.client('s3',
                                      endpoint_url=f"http://{MINIO_ENDPOINT}",
                                      aws_access_key_id=MINIO_ACCESS_KEY,
                                      aws_secret_access_key=MINIO_SECRET_KEY)

        # Check if bucket exists, create if not
        try:
            minio_client.head_bucket(Bucket=bucket_name)
        except Exception:
            minio_client.create_bucket(Bucket=bucket_name)

        # Upload file
        minio_client.upload_file(file_path, bucket_name, object_name)
        logging.info("%s uploaded to MinIO bucket '%s' as '%s'", file_path, bucket_name, object_name)
        return True
    except Exception as exc:
        logging.error("Error uploading to MinIO: %s", exc)
        return False


if __name__ == "__main__":
    if upload_to_minio(FILE_PATH, BUCKET_NAME, OBJECT_NAME):
        logging.info("Upload successful")
    else:
        logging.error("Upload failed")
