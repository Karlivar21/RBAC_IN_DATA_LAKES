# Imports
import io
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from cryptography.fernet import Fernet
from io import BytesIO
from dotenv import load_dotenv
import os
from constants import *

def list_s3_bucket_objects(s3_client):
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    if 'Contents' in response:
        print(f"Objects in bucket '{BUCKET_NAME}':")
        for obj in response['Contents']:
            print(f" - {obj['Key']}")
    else:
        print(f"No objects found in bucket '{BUCKET_NAME}'.")

def retrieve_data(s3_client, file_name):
    """Read a Parquet file from S3"""
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_name)
    data = BytesIO(response['Body'].read())
    df = pd.read_parquet(data, engine="pyarrow")
    print(f"\n successfully loaded '{file_name}' into DataFrame.")
    return df


def encrypt_column_in_parquet(df, column_to_encrypt, fernet_key):
    """Encrypt a specific column in a Parquet file and upload the result to S3."""
    fernet = Fernet(fernet_key)
    print(f"Encrypting column: {column_to_encrypt}")
    if column_to_encrypt in df.columns:
        df[column_to_encrypt] = df[column_to_encrypt].astype(str).apply(
            lambda x: fernet.encrypt(x.encode()).decode()
        )
    else:
        raise ValueError(f"Column '{column_to_encrypt}' not found in Parquet file")
    return df



