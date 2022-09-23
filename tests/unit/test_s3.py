from moto import mock_s3

from select_plus.src.aws.s3 import S3
from tests.util.test_wrapper import TestWrapper


@mock_s3
class TestS3(TestWrapper):

    def test_put_object(self):
        objects_in_bucket = self.s3.list_objects(bucket_name='test-bucket',
                                                 prefix='test')
        self.assertEqual(objects_in_bucket['total_files'], 1)

    def test_list_objects(self):
        objects_in_bucket = self.s3.list_objects(bucket_name='test-bucket',
                                                 prefix='test')
        keys = ['test-key/file.json']
        self.assertEqual(objects_in_bucket['keys'], keys)

    def test_list_objects_with_many_objects(self):
        for i in range(2000):
            self.s3.put_object(bucket_name='test-bucket',
                               key=f'test/test{i}.json',
                               body=b'{"test": 1}')

        objects_in_bucket = self.s3.list_objects(bucket_name='test-bucket',
                                                 prefix='test')

        self.assertEqual(len(objects_in_bucket['keys']), 2001)

    def test_list_objects_response(self):
        objects_in_bucket = self.s3.list_objects(bucket_name='test-bucket',
                                                 prefix='test')
        expected_response = {'keys': ['test-key/file.json'], 'total_file_size': 11, 'total_files': 1}
        self.assertEqual(objects_in_bucket, expected_response)

    # NO IMPLEMENTATION ON MOTO FOR S3 SELECT YET
    # A mocked class has been created to make sure the functionality is correct in the S3 class
    def test_select_json(self):
        s3 = S3(client=self.mock_s3_client)

        response = s3.select(
            bucket_name='test-bucket',
            key='test/test.json',
            sql_string='SELECT * FROM s3object[*] s',
            input_serialization={},
            output_serialization={}
        )

        expected_response = {'payload': 'test',
                             'stats': {'bytes_scanned': 1, 'bytes_processed': 2, 'bytes_returned': 3}}

        self.assertDictEqual(response, expected_response)
