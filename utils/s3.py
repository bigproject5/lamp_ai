# utils/s3.py
from urllib.parse import urlparse
import boto3
import os

def parse_s3_uri(s3_uri: str):
    if not s3_uri.startswith("s3://"):
        raise ValueError("s3Uri must start with s3://")
    p = urlparse(s3_uri)
    bucket = p.netloc
    key = p.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return bucket, key

def _s3_client(region: str | None = None):
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    session_params = {
        "region_name": region or os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    }

    if aws_access_key_id and aws_secret_access_key:
        session_params["aws_access_key_id"] = aws_access_key_id
        session_params["aws_secret_access_key"] = aws_secret_access_key

    return boto3.session.Session(**session_params).client("s3")

def download_s3_to_path(s3_uri: str, local_path: str, region: str | None = None):
    bucket, key = parse_s3_uri(s3_uri)
    _s3_client(region).download_file(bucket, key, local_path)
    return local_path
