import json
import gzip
import logging
import os
import boto3
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
OUTPUT_PREFIX = os.getenv("LAMBDA_OUTPUT_PREFIX", "raw-data/")


def decompress(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    with gzip.GzipFile(fileobj=BytesIO(body)) as gz:
        return gz.read()


def lambda_handler(event, _context):
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        if not key.endswith(".gz"):
            logger.warning(f"Skipping non-gz: {key}")
            return {"statusCode": 200, "body": json.dumps(f"Skipped: {key}")}

        out_key = f"{OUTPUT_PREFIX}{key.split('/')[-1].replace('.gz', '')}"
        data = decompress(bucket, key)
        s3.put_object(Bucket=bucket, Key=out_key, Body=data)

        return {
            "statusCode": 200,
            "body": json.dumps({"src": key, "dst": out_key}),
        }

    except KeyError as e:
        logger.error(f"Invalid event: {e}")
        return {"statusCode": 400, "body": json.dumps(f"Invalid event: {e}")}
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
