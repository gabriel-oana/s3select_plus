from moto import mock_s3
from tests.util.test_wrapper import TestWrapper
from select_plus.src.engine.sequential_engine import SequentialEngine


@mock_s3
class TestSequentialEngine(TestWrapper):

    def test_execute_with_verbose_flag(self):
        sequential_engine = SequentialEngine(
            bucket_name='test',
            prefix='test',
            threads=1,
            verbose=True
        )

        response = sequential_engine.execute(
            sql_query='',
            s3_client=self.mock_s3_client,
            input_serialization={},
            output_serialization={}
        )
        expected_response = [{'payload': 'test', 'stats': {'bytes_scanned': 1, 'bytes_processed': 2, 'bytes_returned': 3}}]
        self.assertListEqual(expected_response, response)

    def test_execute_without_verbose_flag(self):
        sequential_engine = SequentialEngine(
            bucket_name='test',
            prefix='test',
            threads=1,
            verbose=False
        )

        response = sequential_engine.execute(
            sql_query='',
            s3_client=self.mock_s3_client,
            input_serialization={},
            output_serialization={}
        )
        expected_response = [{'payload': 'test', 'stats': {'bytes_scanned': 1, 'bytes_processed': 2, 'bytes_returned': 3}}]
        self.assertListEqual(expected_response, response)

    def test_execute_with_extra_function(self):
        sequential_engine = SequentialEngine(
            bucket_name='test',
            prefix='test',
            threads=1,
            verbose=False
        )

        def extra_func(response, extra_field: str):
            response = f'{response}-{extra_field}'
            return response

        result = sequential_engine.execute(
            sql_query='',
            s3_client=self.mock_s3_client,
            extra_func=extra_func,
            extra_func_args={"extra_field": "test"},
            input_serialization={},
            output_serialization={}
        )

        expected_response = [
            {'stats': {'bytes_scanned': 1, 'bytes_processed': 2, 'bytes_returned': 3}, 'payload': 'test-test'}
        ]

        self.assertListEqual(expected_response, result)
