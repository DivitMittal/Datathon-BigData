import os
from typing import Final

# S3
S3_BUCKET: Final[str] = os.getenv("S3_BUCKET", "test-buckets165")
S3_RAW_DATA_PREFIX: Final[str] = os.getenv("S3_RAW_DATA_PREFIX", "raw-data/")
S3_PROCESSED_DATA_PREFIX: Final[str] = os.getenv(
    "S3_PROCESSED_DATA_PREFIX", "processed-data/"
)
S3_INPUT_PATH: Final[str] = f"s3://{S3_BUCKET}/{S3_RAW_DATA_PREFIX}"
S3_OUTPUT_PATH: Final[str] = f"s3://{S3_BUCKET}/{S3_PROCESSED_DATA_PREFIX}"

# Event filtering (June 26 - July 1, 2024)
EVENT_TIMESTAMP_START: Final[int] = int(
    os.getenv("EVENT_TIMESTAMP_START", "1719340200")
)
EVENT_TIMESTAMP_END: Final[int] = int(os.getenv("EVENT_TIMESTAMP_END", "1719858599"))

# Glue job settings
GLUE_VERSION: Final[str] = os.getenv("GLUE_VERSION", "4.0")
WORKER_TYPE: Final[str] = os.getenv("WORKER_TYPE", "G.2X")
NUMBER_OF_WORKERS: Final[int] = int(os.getenv("NUMBER_OF_WORKERS", "10"))
IDLE_TIMEOUT: Final[int] = int(os.getenv("IDLE_TIMEOUT", "2880"))
JOB_NAME: Final[str] = os.getenv("GLUE_JOB_NAME", "BobbleAI-ETL")
LAMBDA_OUTPUT_PREFIX: Final[str] = os.getenv("LAMBDA_OUTPUT_PREFIX", S3_RAW_DATA_PREFIX)

# Column names
COLUMN_EVENT_ACTION = "event_action"
COLUMN_CREATED_AT = "created_at"
COLUMN_SERVER_CREATED_AT = "server_created_at"
COLUMN_EVENT_TIMESTAMP = "event_timestamp"
COLUMN_EVENT_NAME = "event_name"
COLUMN_EVENT_DATE = "event_date"
COLUMN_PROCESS_DATE = "process_date"
COLUMN_DATA = "data"
COLUMN_REQUEST_ID = "request_id"
COLUMN_DATA_EVENT_PARAMS = "data_event_params"
DATA_FIELD_PREFIX = "data_event_params_exploded_"
