import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
from src.utils import configLogger
import os

from config import (SEAWEED_ENDPOINT, SIM_BUCKET, 
                    AWS_SECRET_KEY, AWS_ACCESS_KEY,
                      REGION_NAME)

logger = configLogger(__file__)


def init_seaweedfs():
    """Idempotent setup for the SeaweedFS S3 bucket."""
    logger.info("--- Connecting to SeaweedFS ---")
    s3 = boto3.client(
        's3',
        endpoint_url=SEAWEED_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name=REGION_NAME
    )

    try:
        s3.head_bucket(Bucket=SIM_BUCKET)
        logger.info(f"SeaweedFS bucket '{SIM_BUCKET}' already exists.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=SIM_BUCKET)
            logger.info(f"SeaweedFS bucket '{SIM_BUCKET}' created.")
        else:
            logger.error(f"SeaweedFS check failed unexpectedly: {e}")
            raise

if __name__ == "__main__":
    print("Initializing infrastructure...")
    init_seaweedfs()
    print("Seaweed infrastructure primed.")