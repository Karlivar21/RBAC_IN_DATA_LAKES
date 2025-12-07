# Imports
import io
from io import BytesIO
import os
from constants import *
import pandas as pd

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

