import boto3
import pandas as pd
from io import BytesIO
from botocore.exceptions import NoCredentialsError, ClientError

BUCKET_NAME = "rher-s3-test-bucket"

def list_s3_objects(bucket_name):
    try:
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            print(f"No objects found in bucket: {bucket_name}")
            return []
        print(f"Objects in bucket '{bucket_name}':")
        for obj in response['Contents']:
            print(f" - {obj['Key']} (LastModified: {obj['LastModified']}, Size: {obj['Size']} bytes)")
        return [obj['Key'] for obj in response['Contents']]

    except NoCredentialsError:
        print("AWS credentials not found. Please configure them first.")
        return []
    except ClientError as e:
        print(f" AWS Client Error: {e}")
        return []
    except Exception as e:
        print(f" Unexpected error: {e}")
        return []

def read_s3_parquet(bucket_name, object_key):
    """Read a Parquet file from S3 and print a preview."""
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        data = BytesIO(response['Body'].read())
        df = pd.read_parquet(data, engine="pyarrow")
        print(f"\n successfully loaded '{object_key}' into DataFrame.")
        print(" Data Preview:")
        print(df)  # Show first few rows
        print(f"DataFrame shape: {df.shape}")

    except ImportError:
        print("You need to install 'pyarrow' or 'fastparquet' to read Parquet files:")
        print("    pip install pyarrow")
    except ClientError as e:
        print(f"Could not read file '{object_key}': {e}")
    except Exception as e:
        print(f"Unexpected error reading Parquet: {e}")

if __name__ == "__main__":
    keys = list_s3_objects(BUCKET_NAME)
    if keys:
        parquet_files = [k for k in keys if k.endswith(".parquet")]
        if parquet_files:
            read_s3_parquet(BUCKET_NAME, parquet_files[0])
        else:
            print("No Parquet files found in the bucket.")
