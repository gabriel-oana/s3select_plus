from select_plus.src.engine.engine import EngineWrapper, EngineResults
from tests.util.test_wrapper import TestWrapper


class TestEngineWrapper(TestWrapper):

    def test_execute_response_type(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine,
                                          input_serialization={},
                                          output_serialization={})
        self.assertIsInstance(response, EngineResults)

    def test_response_payload_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine,
                                          input_serialization={},
                                          output_serialization={})
        self.assertListEqual(response.payload, ['test'])

    def test_response_cost_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine,
                                          input_serialization={},
                                          output_serialization={})
        self.assertEqual(response.stats.cost, 6.77e-11)

    def test_response_files_processed_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine,
                                          input_serialization={},
                                          output_serialization={})
        self.assertEqual(response.stats.files_processed, 1)

    def test_response_bytes_scanned_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine,
                                          input_serialization={},
                                          output_serialization={})
        self.assertEqual(response.stats.bytes_scanned, 30)

    def test_response_bytes_returned_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine,
                                          input_serialization={},
                                          output_serialization={})
        self.assertEqual(response.stats.bytes_returned, 10)

    def test_response_bytes_processed_is_correct(self):
        engine_wrapper = EngineWrapper()
        response = engine_wrapper.execute(sql_query='',
                                          extra_func=None,
                                          extra_func_args=None,
                                          engine=self.mock_engine,
                                          input_serialization={},
                                          output_serialization={})
        self.assertEqual(response.stats.bytes_processed, 30)