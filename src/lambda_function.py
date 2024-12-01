import json
import gzip
import boto3
from pathlib import Path
from io import BytesIO

s3 = boto3.client('s3')

def decompress(bucket_name, object_key):
    object_file = s3.get_object(Bucket=bucket_name, Key=object_key)
    object_body = object_file['Body'].read()

    with gzip.GzipFile(fileobj=BytesIO(object_body)) as gz:
        uncompressed_data = gz.read()

    return uncompressed_data

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        uncompressed_object_key = "raw-data/"+object_key.split('/')[-1].replace('.gz', '')


        s3.put_object(
            Bucket=bucket_name,
            Key=uncompressed_object_key,
            Body=decompress(bucket_name, object_key)
        )

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully unzipped {object_key} to {uncompressed_object_key}')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing {object_key}: {str(e)}')
        }