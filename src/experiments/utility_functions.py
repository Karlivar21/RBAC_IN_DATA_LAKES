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

def retrieve_data(bucket_name, file_name):
    """Read a Parquet file from S3"""
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
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

    
# def upload_encrypted_parquet_to_s3(bucket_name, output_file_location, df):
#     """Upload the encrypted DataFrame as a Parquet file to S3."""
#     s3 = boto3.client("s3")
#     buffer = io.BytesIO()
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, buffer)
#     buffer.seek(0)
#     s3.put_object(Bucket=bucket_name, Key=output_file_location, Body=buffer.getvalue())
#     print(f"Encrypted Parquet file uploaded to s3://{bucket_name}/{output_file_location}")
    

def upload_encrypted_parquet_to_s3(bucket_name, output_file_location, df,
                                   salary_key: bytes, admin_key: bytes):
    """Upload encrypted DataFrame as Parquet to S3 with KMS-style metadata."""

    # 1) Wrap both DEKs with "KMS"
    encrypted_salary_key = kms_encrypt_data_key(salary_key)
    encrypted_admin_key = kms_encrypt_data_key(admin_key)

    table = pa.Table.from_pandas(df)
    existing_md = table.schema.metadata or {}

    kms_metadata = {
        b"kms_key_id": KMS_KEY_ID.encode("utf-8"),
        b"encrypted_data_key_salary": encrypted_salary_key,
        b"encrypted_data_key_admin": encrypted_admin_key,
        b"encryption_algorithm_salary": b"FERNET",
        b"encryption_algorithm_admin": b"FERNET",
        b"encryption_schema_version": b"1",
    }

    new_md = {**existing_md, **kms_metadata}
    table = table.replace_schema_metadata(new_md)

    # 3) Write to S3
    s3 = boto3.client("s3")
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3.put_object(Bucket=bucket_name, Key=output_file_location, Body=buffer.getvalue())
    print(f"Encrypted Parquet file uploaded to s3://{bucket_name}/{output_file_location}")


