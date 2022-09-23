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

