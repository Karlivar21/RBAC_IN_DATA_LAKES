import boto3

s3 = boto3.client("s3")
BUCKET_NAME = "rher-s3-test-bucket"
PREFIX = ""  # just the folder part

response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
for obj in response.get("Contents", []):
    print(obj["Key"])
