import boto3
import os

# MinIO configuration
minio_endpoint = os.environ.get('MINIO_ENDPOINT', 'localhost:9000')
minio_access_key = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
minio_secret_key = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
bucket_name = 'sales-data'
object_name = 'sales_data.csv'
file_path = 'test_data/sales_data.csv'.replace('/', '\\')

# Create MinIO client
minio_client = boto3.client('s3',
                      endpoint_url=f"http://{minio_endpoint}",
                      aws_access_key_id=minio_access_key,
                      aws_secret_access_key=minio_secret_key)

def upload_to_minio(file_path, bucket_name, object_name):
    try:
        # Check if bucket exists, create if not
        try:
            minio_client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            minio_client.create_bucket(Bucket=bucket_name)

        # Upload file
        minio_client.upload_file(file_path, bucket_name, object_name)
        print(f"{file_path} uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
        return False
    return True

if __name__ == "__main__":
    if upload_to_minio(file_path, bucket_name, object_name):
        print("Upload successful")
    else:
        print("Upload failed")
