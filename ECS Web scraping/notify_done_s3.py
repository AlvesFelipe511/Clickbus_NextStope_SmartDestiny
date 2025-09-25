# notify_done_s3.py
import os
import datetime
import boto3

region = os.getenv("AWS_REGION", "sa-east-1")
bucket = os.getenv("S3_BUCKET", "cbchallenge")
prefix = os.getenv("MARKER_PREFIX", "Bronze/_markers/")

s3 = boto3.client("s3", region_name=region)

stamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
key = f"{prefix}{stamp}.done"

s3.put_object(Bucket=bucket, Key=key, Body=b"")
print(f"[DONE] Sentinela criado: s3://{bucket}/{key}")
