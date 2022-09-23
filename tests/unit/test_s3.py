import unittest
import boto3
from moto import mock_s3
from select_plus.src.aws.s3 import S3


@mock_s3
class TestS3(unittest.TestCase):

    def setUp(self) -> None:
        # Create mock client
        client = boto3.client("s3", region_name='eu-west-1')
        self.s3 = S3(client=client)

        # Create mock bucket
        client.create_bucket(Bucket='test-bucket',
                             CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})

    def test_put_object(self):
        self.s3.put_object(bucket_name='test-bucket',
                           key='test/test.json',
                           body=b'test-string')

        objects_in_bucket = self.s3.list_objects(bucket_name='test-bucket',
                                                 prefix='test')
        self.assertEqual(objects_in_bucket['total_files'], 1)

    def test_list_objects(self):
        self.s3.put_object(bucket_name='test-bucket',
                           key='test/test.json',
                           body=b'{"test": 1}')

        objects_in_bucket = self.s3.list_objects(bucket_name='test-bucket',
                                                 prefix='test')
        keys = ['test/test.json']
        self.assertEqual(objects_in_bucket['keys'], keys)

    def test_list_objects_with_many_objects(self):
        for i in range(2000):
            self.s3.put_object(bucket_name='test-bucket',
                               key=f'test/test{i}.json',
                               body=b'{"test": 1}')

        objects_in_bucket = self.s3.list_objects(bucket_name='test-bucket',
                                                 prefix='test')

        self.assertEqual(len(objects_in_bucket['keys']), 2000)

    def test_list_objects_response(self):
        self.s3.put_object(bucket_name='test-bucket',
                           key='test/test.json',
                           body=b'{"test": 1}')

        objects_in_bucket = self.s3.list_objects(bucket_name='test-bucket',
                                                 prefix='test')
        expected_response = {
            'keys': ['test/test.json'],
            'total_file_size': 14,
            'total_files': 1
        }
        self.assertEqual(objects_in_bucket, expected_response)

    # NO IMPLEMENTATION ON MOTO FOR S3 SELECT YET
    # A mocked class has been created to make sure the functionality is correct in the S3 class
    def test_select_json(self):

        class Client:

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

        s3 = S3(client=Client())

        response = s3.select(
            bucket_name='test-bucket',
            key='test/test.json',
            sql_string='SELECT * FROM s3object[*] s'
        )

        expected_response = {'payload': 'test', 'stats': {'bytes_scanned': 1, 'bytes_processed': 2, 'bytes_returned': 3}}

        self.assertDictEqual(response, expected_response)

