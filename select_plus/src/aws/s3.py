from typing import Any
import boto3


class S3:

    def __init__(self, client: boto3.session.Session.client = None):
        self.client = client if client else boto3.client('s3')

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

            for content in page['Contents']:
                keys.append(content['Key'])
                total_file_size += content['Size']

        response = {
            "total_files": total_files,
            "total_file_size": total_file_size,
            "keys": keys
        }
        return response

    def select(self, bucket_name: str, key: str, sql_string: str):
        response = self.client.select_object_content(
            Bucket=bucket_name,
            Key=key,
            Expression=sql_string,
            ExpressionType='SQL',
            RequestProgress={
                'Enabled': True
            },
            InputSerialization={
                'CompressionType': 'NONE',
                'JSON': {
                    'Type': 'DOCUMENT'
                }
            },
            OutputSerialization={
                'JSON': {
                    'RecordDelimiter': '\n'
                }
            },
        )

        payload = list(response['Payload'])
        content = {
            "payload": payload[0]['Records']['Payload'].decode(),
            "stats": {
                "bytes_scanned": payload[1]['Stats']['Details']['BytesScanned'],
                "bytes_processed": payload[1]['Stats']['Details']['BytesProcessed'],
                "bytes_returned": payload[1]['Stats']['Details']['BytesReturned'],
            }
        }

        return content


if __name__ == '__main__':
    from pprint import pprint
    s3 = S3()
    # results = s3.list_objects(bucket_name='prd-cipher-finance-raw', prefix='hourly')
    # pprint(results)

    results = s3.select(
        bucket_name='prd-cipher-finance-raw',
        key='hourly/aiai/2022/05/03/aiai.json',
        sql_string="SELECT * FROM s3object[*] s"
    )

    print(results)