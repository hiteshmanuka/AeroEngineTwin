import os
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
SIM_BUCKET = os.environ.get("RAW_SIM_BUCKET")
AWS_ACCESS_KEY = os.environ.get("SEAWEED_AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("SEAWEED_AWS_SECRET_KEY")
REGION_NAME= os.environ.get("REGION_NAME")

#ingestion-encoding
ENCODING=os.environ.get("ENCODING")

#downsampling
MIN_BUCKETS=int(os.environ.get("MIN_BUCKETS"))
MAX_BUCKETS=int(os.environ.get("MAX_BUCKETS"))
DEFAULT_BUCKETS=int(os.environ.get("DEFAULT_BUCKETS"))
DEFAULT_START=int(os.environ.get("DEFAULT_START"))
DEFAULT_END=int(os.environ.get("DEFAULT_END"))