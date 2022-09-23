import unittest
import boto3
from moto import mock_s3
from select_plus.src.aws.s3 import S3


class MockS3Paginator:

    @staticmethod
    def paginate(Bucket: str, Prefix: str):
        payload = [
            {
                'KeyCount': 1,
                'Contents': [{
                    "Key": 'test.json',
                    "Size": 123
                }]
            }
        ]

        return payload


class MockS3Client:

    @staticmethod
    def select_object_content(*args, **kwargs):
        payload = [
            {
                "Records": {
                    "Payload": str('test').encode()
                }
            },
            {
                "Stats": {
                    "Details": {
                        "BytesScanned": 1,
                        "BytesProcessed": 2,
                        "BytesReturned": 3
                    }
                }
            }
        ]

        mock_response = {"Payload": payload}

        return mock_response

    @staticmethod
    def get_paginator(*args, **kwargs):
        return MockS3Paginator()


@mock_s3
class TestWrapper(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_s3_client = MockS3Client()
        self.client = boto3.client("s3", region_name='eu-west-1')

        # Create mock bucket
        self.client.create_bucket(Bucket='test-bucket',
                                  CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        self.s3 = S3(client=self.client)

