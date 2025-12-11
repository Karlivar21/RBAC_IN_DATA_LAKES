
from constants import *
from utility_functions import *
import pyarrow as pa

class DataAccessLayerClient:
    """
    Hypothetical DAL client

    In the ideal world, both Snowflake and Python would effectively
    call this logic whenever they read data from S3.
    """
    def __init__(self, base_url):
        self.base_url = base_url
        
    def authenticate_user(self, token):
        print("Authenticating user with token:", token)
        """
        Simulates user authentication.
        In a real DAL, this would verify the token and return user info.
        return credentials for role
        """
        if not token:
            raise Exception("Authentication failed: No token provided")
        elif token == TOKEN_ENGINEER:
            return {"user_id": "engineer_user", "roles": [ROLE_ENGINEER]}
        elif token == TOKEN_HR:
            return {"user_id": "hr_user", "roles": [ROLE_HR]}
        elif token == TOKEN_ADMIN:
            return {"user_id": "admin_user", "roles": [ROLE_ADMIN]}
        else:
            raise Exception("Authentication failed: Invalid token")

    
    def get_data(self, token, file_name):
        """
        Simulates reading a Parquet file with DAL logic. That is, it simulates the above behaviour:
        - authenticate the user and gets the user role
        - gets credentials for roles
        - establishes connection towards the datalake and the kms
        - fetch Parquet with name from underlying bucket and 
        - decrypt protected columns if allowed
        - return a DataFrame or bytes of a Parquet file
        """
        role = self.authenticate_user(token)["roles"][0]
        role_credentials = get_aws_credentials(role)
        kms_client = create_aws_client_for(role_credentials, "kms")
        s3_client = create_aws_client_for(role_credentials, "s3")
        file = get_data(s3_client, file_name)
        return decrypt_parquet(file, kms_client)
   
    def put_data(self, token, data, file_name):
        """
        Simulates writing a Parquet file with DAL logic. That is, it simulates the above behaviour:
        - authenticate the user and gets the user role
        - gets credentials for roles
        - establishes connection towards the datalake and the kms
        - encrypt protected columns if allowed
        - upload Parquet to underlying bucket with name

        Note: This method assumes data is a Pandas dataframe.
        """
        role = self.authenticate_user(token)["roles"][0]
        if role != ROLE_ADMIN:
            raise Exception(f"Permission denied: {role} role cannot write data")
        role_credentials = get_aws_credentials(role)
        kms_client = create_aws_client_for(role_credentials, "kms")
        s3_client = create_aws_client_for(role_credentials, "s3")
        parquet_data = encrypt_parquet(data, kms_client)
        s3_client.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=parquet_data.getvalue())
        print(f"Successfully uploaded '{file_name}' to S3 bucket '{BUCKET_NAME}'")

    