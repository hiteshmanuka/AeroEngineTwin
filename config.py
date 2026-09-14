import os
from datetime import timedelta

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

#TIMESCALEDDB
USER = os.environ.get("POSTGRES_USER")
PASSWORD = os.environ.get("POSTGRES_PASSWORD")
HOST = os.environ.get("POSTGRES_HOST")
PORT = os.environ.get("POSTGRES_PORT")
DB = os.environ.get("POSTGRES_DB")

# example: postgresql://user:password@localhost:5433/aeroenginetwin
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
MINSIZE=3
MAXSIZE=6

#SEAWEEDFS
SEAWEED_ENDPOINT = os.environ.get("SEAWEED_ENDPOINT")
AWS_ACCESS_KEY = os.environ.get("SEAWEED_AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("SEAWEED_AWS_SECRET_KEY")
REGION_NAME= "us-east-1"