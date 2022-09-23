from moto import mock_s3
from tests.util.test_wrapper import TestWrapper
from select_plus.src.engine.parallel_engine import ParallelEngine


# Do not put this function inside the test class. It cannot be pickled
def func(n: int):
    return n * n


@mock_s3
class TestParallelEngine(TestWrapper):

    def test_execute_callable_verbose(self):
        parallel_engine = ParallelEngine(
            bucket_name='test',
            prefix='test',
            threads=1,
            verbose=True
        )

        response = parallel_engine.execute_callable(
            func=func,
            args=[1, 2, 3, 4]
        )
        expected_response = [1, 4, 9, 16]
        self.assertListEqual(expected_response, response)

    def test_execute_callable(self):
        parallel_engine = ParallelEngine(
            bucket_name='test',
            prefix='test',
            threads=1,
            verbose=False
        )

        response = parallel_engine.execute_callable(
            func=func,
            args=[1, 2, 3, 4]
        )

        expected_response = [1, 4, 9, 16]
        self.assertListEqual(expected_response, response)

    def test_make_func_args(self):
        parallel_engine = ParallelEngine(
            bucket_name='test-bucket',
            prefix='test-key',
            threads=1,
            verbose=False
        )

        args_list = parallel_engine._make_func_args(
            sql_query='SELECT * FROM s3object s',
            extra_func=None,
            extra_func_args={'test': 1},
            s3_client=self.client,
            input_serialization={},
            output_serialization={}
        )

        args_list[0]['s3_client'] = None

        expected_args = [
            {
                'key': 'test-key/file.json',
                'sql_query': 'SELECT * FROM s3object s',
                'extra_func': None,
                's3_client': None,
                'extra_func_args': {'test': 1},
                'input_serialization': {},
                'output_serialization': {}
            }
        ]

        self.assertListEqual(args_list, expected_args)