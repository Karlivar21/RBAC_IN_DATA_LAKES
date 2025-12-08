
# S3 constants
BUCKET_NAME = "s3-rbac-in-data-lakes-experiments"
ACCOUNT_ID = "501994300007" 
REGION = "eu-north-1"

# Encrypted column KMS Key IDs
SALARY_KEY_ID = "86c41c3f-fc21-4730-a20b-b755e5b63ebb"
PASSWORD_KEY_ID = "5722129f-2136-4ef4-8b53-5a242b553f34"
FILE_ACCESS_KEY_ID = "d229ff0a-b839-4732-9dd8-602c38a4487b"

# File keys
EMPLOYEE_DATA_RAW_KEY = "employee_data_raw.parquet"
EMPLOYEE_DATA_ENCRYPTED_LOCALLY_KEY = "employee_data_encrypted_locally.parquet"
EMPLOYEE_DATA_ENCRYPTED_MODULAR_KEY = "employee_data_encrypted_modular.parquet"

# File paths
EMPLOYEE_DATA_RAW_CSV_PATH = "../data/employee_data_raw.csv"
EMPLOYEE_DATA_RAW_PARQUET_PATH = "../data/employee_data_raw.parquet"
EMPLOYEE_DATA_ENCRYPTED_LOCALLY_PATH = "../data/employee_data_encrypted_locally.parquet"
EMPLOYEE_DATA_ENCRYPTED_MODULAR_PATH = "../data/employee_data_encrypted_modular.parquet"

# Roles
ROLE_ENGINEER = "ENGINEER"
ROLE_HR = "HR"
ROLE_ADMIN = "ADMIN"