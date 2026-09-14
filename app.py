from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

import boto3
from botocore.client import Config
import asyncpg

import os
from config import (DATABASE_URL, 
                    MINSIZE, MAXSIZE,
                    AWS_SECRET_KEY, AWS_ACCESS_KEY,
                    SEAWEED_ENDPOINT, REGION_NAME)

from src.utils import configLogger

logger = configLogger("fastapi.app")

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        logger.info("Initializing TimescaleDB connection pool")
        app.state.db_pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=MINSIZE,
            max_size=MAXSIZE
        )
        
        logger.info("Initializing SeaweedFS S3 client")
        app.state.s3_client = boto3.client(
            's3',
            endpoint_url=SEAWEED_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name=REGION_NAME
        )
        
        yield

        logger.info("Shutting down connections...")
        await app.state.db_pool.close()

    except Exception as e:
        logger.exception(f"lifespan creation failed : {Exception}")
        raise


app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "Welcome to the App!"}
