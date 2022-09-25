from typing import Any
import boto3
from botocore.config import Config


class S3:

    def __init__(self, client: boto3.session.Session.client = None):
        self.config = Config(
            retries=dict(max_attempts=10)
        )
        self.client = client if client else boto3.client('s3', config=self.config)

    def put_object(self, bucket_name: str, key: str, body: Any):
        """
        Puts object into S3
        """
        self.client.put_object(Body=str(body).encode(), Bucket=bucket_name, Key=key)

    def list_objects(self, bucket_name: str, prefix: str):
        """
        Lists all objects in S3 with a prefix.
        """

        total_files = 0
        total_file_size = 0
        keys = []

        paginator = self.client.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            total_files += page['KeyCount']

            if total_files == 0:
                raise RuntimeError(f'No files found in prefix {prefix}')

            for content in page['Contents']:
                keys.append(content['Key'])
                total_file_size += content['Size']

        response = {
            "total_files": total_files,
            "total_file_size": total_file_size,
            "keys": keys
        }
        return response

    def select(self,
               bucket_name: str,
               key: str,
               sql_string: str,
               input_serialization: dict,
               output_serialization: dict
               ) -> dict:

        response = self.client.select_object_content(
            Bucket=bucket_name,
            Key=key,
            Expression=sql_string,
            ExpressionType='SQL',
            RequestProgress={
                'Enabled': True
            },
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization
        )

        payload = list(response['Payload'])

        full_content = []
        bytes_scanned = None
        bytes_processed = None
        bytes_returned = None

        for item in payload:
            if "Records" in item.keys():
                full_content.append(item['Records']['Payload'].decode())
            if "Stats" in item.keys():
                bytes_scanned = item['Stats']['Details']['BytesScanned']
                bytes_processed = item['Stats']['Details']['BytesProcessed']
                bytes_returned = item['Stats']['Details']['BytesReturned']

        content = {
            "payload": ''.join(full_content),
            "stats": {
                "bytes_scanned": bytes_scanned,
                "bytes_processed": bytes_processed,
                "bytes_returned": bytes_returned,
            }
        }

        return content

