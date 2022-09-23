import unittest

from select_plus.src.utils.cost import Cost


class TestCost(unittest.TestCase):

    def test_compute_block(self):
        cost = Cost()
        result = cost.compute_block(
            data_scanned=100000,
            data_returned=10000,
            files_requested=10
        )
        self.assertEqual(result, 2.07007e-07)

    def test_compute_stack(self):
        cost = Cost()
        result = cost.compute_stack([1, 2, 3])
        self.assertEqual(result, 6)