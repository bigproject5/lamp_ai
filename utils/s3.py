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

def parse_s3_https_url(https_url: str):
    """HTTPS S3 URL을 bucket과 key로 파싱"""
    if not https_url.startswith("https://"):
        raise ValueError("URL must start with https://")

    p = urlparse(https_url)
    # https://bucket-name.s3.amazonaws.com/key 형식 파싱
    if ".s3.amazonaws.com" in p.netloc:
        bucket = p.netloc.split(".s3.amazonaws.com")[0]
        key = p.path.lstrip("/")
        return bucket, key
    else:
        raise ValueError(f"Invalid S3 HTTPS URL: {https_url}")

def s3_uri_to_https_url(s3_uri: str, region: str = "ap-northeast-2"):
    """S3 URI를 HTTPS URL로 변환"""
    bucket, key = parse_s3_uri(s3_uri)
    return f"https://{bucket}.s3.amazonaws.com/{key}"

def https_url_to_s3_uri(https_url: str):
    """HTTPS URL을 S3 URI로 변환"""
    bucket, key = parse_s3_https_url(https_url)
    return f"s3://{bucket}/{key}"

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

def upload_file_to_s3(local_path: str, s3_uri: str, region: str | None = None):
    """로컬 파일을 S3에 업로드"""
    bucket, key = parse_s3_uri(s3_uri)
    _s3_client(region).upload_file(local_path, bucket, key)
    return s3_uri

def upload_bytes_to_s3(file_bytes: bytes, s3_uri: str, content_type: str = "image/jpeg", region: str | None = None):
    """바이트 데이터를 S3에 업로드하고 HTTPS URL 반환"""
    bucket, key = parse_s3_uri(s3_uri)
    _s3_client(region).put_object(
        Bucket=bucket,
        Key=key,
        Body=file_bytes,
        ContentType=content_type
    )
    # HTTPS URL 형식으로 반환
    return s3_uri_to_https_url(s3_uri)
