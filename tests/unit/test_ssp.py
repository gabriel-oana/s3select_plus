from moto import mock_s3
from select_plus.ssp import SSP
from select_plus.src.engine.engine import EngineResults
from tests.util.test_wrapper import TestWrapper, MockEngine


@mock_s3
class TestSSP(TestWrapper):

    def test_estimate_cost(self):
        ssp = SSP(
            bucket_name='test-bucket',
            prefix='test-key',
            verbose=False,
            engine=MockEngine
        )

        cost = ssp.estimate_cost()
        self.assertEqual(cost, 3.04e-11)

    def test_select_type(self):
        ssp = SSP(
            bucket_name='test-bucket',
            prefix='test-key',
            verbose=False,
            engine=MockEngine
        )

        results = ssp.select(
            sql_query='SELECT * FROM s3object s'
        )

        self.assertIsInstance(results, EngineResults)

    def test_select_results(self):
        ssp = SSP(
            bucket_name='test-bucket',
            prefix='test-key',
            verbose=False,
            engine=MockEngine
        )

        results = ssp.select(
            sql_query='SELECT * FROM s3object s'
        )
        expected_result = results.payload

        self.assertListEqual(expected_result, ['test'])

    def test_select_cost(self):
        ssp = SSP(
            bucket_name='test-bucket',
            prefix='test-key',
            verbose=False,
            engine=MockEngine
        )

        results = ssp.select(
            sql_query='SELECT * FROM s3object s'
        )
        expected_result = results.stats.cost

        self.assertEqual(expected_result, 6.77e-11)

    def test_select_files_processed(self):
        ssp = SSP(
            bucket_name='test-bucket',
            prefix='test-key',
            verbose=False,
            engine=MockEngine
        )

        results = ssp.select(
            sql_query='SELECT * FROM s3object s'
        )
        expected_result = results.stats.files_processed

        self.assertEqual(expected_result, 1)

    def test_select_bytes_processed(self):
        ssp = SSP(
            bucket_name='test-bucket',
            prefix='test-key',
            verbose=False,
            engine=MockEngine
        )

        results = ssp.select(
            sql_query='SELECT * FROM s3object s'
        )
        expected_result = results.stats.bytes_processed

        self.assertEqual(expected_result, 30)

    def test_select_bytes_scanned(self):
        ssp = SSP(
            bucket_name='test-bucket',
            prefix='test-key',
            verbose=False,
            engine=MockEngine
        )

        results = ssp.select(
            sql_query='SELECT * FROM s3object s'
        )
        expected_result = results.stats.bytes_scanned

        self.assertEqual(expected_result, 30)

    def test_select_bytes_returned(self):
        ssp = SSP(
            bucket_name='test-bucket',
            prefix='test-key',
            verbose=False,
            engine=MockEngine
        )

        results = ssp.select(
            sql_query='SELECT * FROM s3object s'
        )
        expected_result = results.stats.bytes_returned

        self.assertEqual(expected_result, 10)

    def test_raises_on_wrong_engine(self):
        class WrongEngine:
            pass

        self.assertRaises(RuntimeError, SSP, bucket_name='test-bucket', prefix='test-key', engine=WrongEngine)

