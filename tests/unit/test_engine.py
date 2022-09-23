import unittest

from select_plus.src.engine.base_engine import BaseEngine
from select_plus.src.engine.engine import EngineWrapper, EngineResults


class TestEngineWrapper(unittest.TestCase):

    def setUp(self) -> None:
        class MockEngine(BaseEngine):
            def execute(self, sql_query: str, extra_func: callable = None, extra_func_args: dict = None):
                response = [
                    {"payload": "test",
                     "stats": {
                        "bytes_scanned": 30,
                        "bytes_processed": 30,
                        "bytes_returned": 10
                        }
                     }
                ]
                return response

        self.mock_engine = MockEngine(
            bucket_name='test',
            prefix='test',
            threads=1,
            verbose=False
        )

    def test_execute_response_type(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine)
        self.assertIsInstance(response, EngineResults)

    def test_response_payload_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine)
        self.assertListEqual(response.payload, ['test'])

    def test_response_cost_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine)
        self.assertEqual(response.stats.cost, 6.77e-11)

    def test_response_files_processed_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine)
        self.assertEqual(response.stats.files_processed, 1)

    def test_response_bytes_scanned_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine)
        self.assertEqual(response.stats.bytes_scanned, 30)

    def test_response_bytes_returned_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine)
        self.assertEqual(response.stats.bytes_returned, 10)

    def test_response_bytes_processed_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine)
        self.assertEqual(response.stats.bytes_processed, 30)