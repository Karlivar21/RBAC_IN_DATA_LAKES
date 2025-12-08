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
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_name)
        data = BytesIO(response['Body'].read())
        print(f"\n successfully loaded '{file_name}' from S3 bucket '{BUCKET_NAME}'")
        return data
    except Exception as e:
        print(f"Failed to retrieve '{file_name}' from S3: {e}")
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
    
def get_keys_for_role(role):
    credentials = get_aws_credentials(role)
    kms_client = create_aws_client_for(credentials, "kms")
    return usable_keys(kms_client)


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

