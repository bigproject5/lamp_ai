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
    return boto3.session.Session(
        region_name=region or os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    ).client("s3")

def download_s3_to_path(s3_uri: str, local_path: str, region: str | None = None):
    bucket, key = parse_s3_uri(s3_uri)
    _s3_client(region).download_file(bucket, key, local_path)
    return local_path
