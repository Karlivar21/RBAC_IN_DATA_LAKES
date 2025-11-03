import io
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from cryptography.fernet import Fernet

BUCKET_NAME = "rher-s3-test-bucket"
INPUT_KEY = "sample_sensitive_data.parquet"
OUTPUT_KEY = "path/to/output_encrypted.parquet"
COLUMN_TO_ENCRYPT = "salary" 
FERNET_KEY = "U1eIY6p4bKjOaMycX1VyMshD0tRmfWqC7xJ0MMT8oO0="
fernet = Fernet(FERNET_KEY)
s3 = boto3.client("s3")
buffer = io.BytesIO()
s3.download_fileobj(BUCKET_NAME, INPUT_KEY, buffer)
buffer.seek(0)
table = pq.read_table(buffer)
df = table.to_pandas()
print(f"Encrypting column: {COLUMN_TO_ENCRYPT}")
if COLUMN_TO_ENCRYPT in df.columns:
    df[COLUMN_TO_ENCRYPT] = df[COLUMN_TO_ENCRYPT].astype(str).apply(
        lambda x: fernet.encrypt(x.encode()).decode()
    )
else:
    raise ValueError(f"Column '{COLUMN_TO_ENCRYPT}' not found in Parquet file")

output_buffer = io.BytesIO()
pq.write_table(pa.Table.from_pandas(df), output_buffer)
output_buffer.seek(0)
print("Uploading encrypted Parquet file to S3...")
s3.upload_fileobj(output_buffer, BUCKET_NAME, OUTPUT_KEY)

print("✅ Done!")
print(f"Encrypted file uploaded to s3://{BUCKET_NAME}/{OUTPUT_KEY}")
