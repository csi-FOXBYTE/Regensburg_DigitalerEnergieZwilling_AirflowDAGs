import os
import boto3
from botocore.config import Config

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_REGION = os.getenv("S3_REGION", "eu-central-1")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "test")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "test")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "external-downloads")
S3_FORCE_PATH_STYLE = os.getenv("S3_FORCE_PATH_STYLE", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _build_s3_client():
    addressing_style = "path" if S3_FORCE_PATH_STYLE else "virtual"
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(s3={"addressing_style": addressing_style}),
    )