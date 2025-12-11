# Imports
from io import BytesIO
import os
import boto3
from botocore.exceptions import ClientError
from constants import *
import AwsKmsClient
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
import pyarrow as pa

def list_s3_bucket_objects(s3_client):
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    if 'Contents' in response:
        print(f"Objects in bucket '{BUCKET_NAME}':")
        for obj in response['Contents']:
            print(f" - {obj['Key']}")
    else:
        print(f"No objects found in bucket '{BUCKET_NAME}'.")

def get_data(s3_client, file_name):
    """Read a Parquet file from S3"""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_name)
        data = BytesIO(response['Body'].read())
        print(f"\n successfully loaded '{file_name}' from S3 bucket '{BUCKET_NAME}'")
        return data
    except Exception as e:
        print(f"Failed to get '{file_name}' from S3: {e}")
        return None
    
def usable_keys(kms):
    usable = []
    keys_resp = kms.list_keys()
    for k in keys_resp["Keys"]:
        key_id = k["KeyId"]
        try:
            # Cheap way to test "can I use this key?"
            kms.generate_data_key(
                KeyId=key_id,
                KeySpec="AES_256"
            )
            usable.append(key_id)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("AccessDeniedException", "AccessDenied"):
                # Not allowed to use this key
                continue
            else:
                raise
    return usable

def get_aws_credentials(role):
    ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/{role}" 
    SESSION_NAME = f"{role}-notebook-session"
    session = boto3.Session(
        aws_access_key_id=os.getenv(f"AWS_{role}_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv(f"AWS_{role}_SECRET_ACCESS_KEY"),
    )
    sts_client = session.client("sts")
    try:
        assumed_role = sts_client.assume_role(
            RoleArn=ROLE_ARN,
            RoleSessionName=SESSION_NAME
        )
        creds = assumed_role["Credentials"]
        print(f"Assumed {role} role successfully.")
        return creds
    except ClientError as e:
        print("Error assuming HR role:", e)
        raise

def create_aws_client_for(creds, service_name):
    return boto3.client(
        service_name,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=REGION
    )

def make_crypto_factory_for_kms(boto3_kms_client) -> pe.CryptoFactory:
    """
    Given a boto3 KMS client (for a specific role), build a CryptoFactory.
    """
    key_map = {
        "file-access-key": f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:key/{FILE_ACCESS_KEY_ID}",
        "salary-key": f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:key/{SALARY_KEY_ID}",
        "password-key": f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:key/{PASSWORD_KEY_ID}",
    }

    def kms_client_factory(kms_connection_config: pe.KmsConnectionConfig):
        return AwsKmsClient.AwsKmsClient(boto3_kms_client, key_map)

    return pe.CryptoFactory(kms_client_factory)

def decrypt_parquet(parquet_file, kms_client):
    """
    Try to read a modular encrypted Parquet file for a given role (KMS client).

    - If the role can decrypt the footer but not all columns:
        Try each column individually and keep only those that can be read.
      * Else:
          Raise as soon as any column fails.

    - If the role cannot decrypt the footer at all (no footer key access),
      opening ParquetFile will raise immediately.
    """
    kms_conn_config = pe.KmsConnectionConfig(
        kms_instance_id=f"aws-kms-{REGION}",
        kms_instance_url=f"https://kms.{REGION}.amazonaws.com"
    )
    crypto_factory = make_crypto_factory_for_kms(kms_client)
    decryption_props = crypto_factory.file_decryption_properties(kms_conn_config)

    try:
        pf = pq.ParquetFile(
            parquet_file,
            decryption_properties=decryption_props
        )
    except Exception as e:
        print(f"Failed to open Parquet file: {e}")
        return None

    schema = pf.schema
    print("File columns:", schema.names)
        
    # Try reading columns one by one
    readable_cols = []
    for name in schema.names:
        try:
            pf.read(columns=[name])  # attempt to read single column
            readable_cols.append(name)
        except Exception as e:
            print(f"Skipping column {name} due to decryption error: {e}")

    if not readable_cols:
        print("No readable columns for this role.")
        return None

    # Finally read only the columns that worked
    table = pf.read(columns=readable_cols)
    df = table.to_pandas()
    return df

def encrypt_parquet(df, kms_client):
    """
    Encrypt a Parquet file with modular encryption using the given KMS client.
    """
    kms_conn_config = pe.KmsConnectionConfig(
        kms_instance_id=f"aws-kms-{REGION}",
        kms_instance_url=f"https://kms.{REGION}.amazonaws.com"
    )
    crypto_factory = make_crypto_factory_for_kms(kms_client)
    encryption_config = pe.EncryptionConfiguration(
        footer_key="file-access-key",              
        column_keys={
            "salary-key": ["Salary"],     
            "password-key": ["Password"] 
        },
        plaintext_footer=False         
    )
    file_encryption_props = crypto_factory.file_encryption_properties(
        kms_conn_config,
        encryption_config
    )

    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(
        table,
        buffer,
        encryption_properties=file_encryption_props
    )
    buffer.seek(0)
    return buffer

