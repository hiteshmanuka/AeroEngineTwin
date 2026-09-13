import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
from src.utils import configLogger
import os

logger = configLogger(__file__)

S3_ENDPOINT = os.environ.get("SEAWEEDFS_ENDPOINT")
BUCKET_NAME = os.environ.get("RAW_SIM_BUCKET")

def init_seaweedfs():
    """Idempotent setup for the SeaweedFS S3 bucket."""
    logger.info("--- Connecting to SeaweedFS ---")
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("SEAWEEDFS_ACCESS_KEY", "any_key"),
        aws_secret_access_key=os.environ.get("SEAWEEDFS_SECRET_KEY", "any_secret"),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"SeaweedFS bucket '{BUCKET_NAME}' already exists.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=BUCKET_NAME)
            logger.info(f"SeaweedFS bucket '{BUCKET_NAME}' created.")
        else:
            logger.error(f"SeaweedFS check failed unexpectedly: {e}")
            raise

if __name__ == "__main__":
    print("Initializing infrastructure...")
    init_seaweedfs()
    print("Seaweed infrastructure primed.")