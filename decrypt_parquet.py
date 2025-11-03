import io
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from cryptography.fernet import Fernet

BUCKET_NAME = "rher-s3-test-bucket"
ENCRYPTED_KEY = "path/to/output_encrypted.parquet"
OUTPUT_KEY_DECRYPTED = "path/to/output_decrypted.parquet"
COLUMN_TO_DECRYPT = "salary"
FERNET_KEY = "U1eIY6p4bKjOaMycX1VyMshD0tRmfWqC7xJ0MMT8oO0="
fernet = Fernet(FERNET_KEY)
s3 = boto3.client("s3")
print("Downloading encrypted Parquet file from S3...")
buffer = io.BytesIO()
s3.download_fileobj(BUCKET_NAME, ENCRYPTED_KEY, buffer)
buffer.seek(0)
table = pq.read_table(buffer)
df = table.to_pandas()

print(f"Decrypting column: {COLUMN_TO_DECRYPT}")
if COLUMN_TO_DECRYPT in df.columns:
    df[COLUMN_TO_DECRYPT] = df[COLUMN_TO_DECRYPT].astype(str).apply(
        lambda x: fernet.decrypt(x.encode()).decode()
    )
else:
    raise ValueError(f"Column '{COLUMN_TO_DECRYPT}' not found in encrypted Parquet file")

output_buffer = io.BytesIO()
pq.write_table(pa.Table.from_pandas(df), output_buffer)
output_buffer.seek(0)

print("Uploading decrypted Parquet file to S3...")
s3.upload_fileobj(output_buffer, BUCKET_NAME, OUTPUT_KEY_DECRYPTED)

print("✅ Done!")

