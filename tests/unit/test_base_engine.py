from unittest.mock import patch

from select_plus.src.engine.base_engine import BaseEngine
from tests.util.test_wrapper import TestWrapper


class TestBaseEngine(TestWrapper):

    def test_instance_raises(self):
        self.assertRaises(TypeError, BaseEngine, bucket_name='test', prefix='test', threads=1, verbose=False)

    @patch.multiple(BaseEngine, __abstractmethods__=set())
    def test_execute_raises(self):

        class TestEngine(BaseEngine):
            pass

        test_engine = TestEngine(bucket_name='test', prefix='test', threads=1, verbose=False)
        self.assertRaises(NotImplementedError, test_engine.execute, sql_query='', input_serialization={},
                          output_serialization={})
