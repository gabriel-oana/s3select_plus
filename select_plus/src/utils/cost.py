

class Cost:

    def __init__(self):
        self.request_cost = 0.0004 / 1000
        self.returned_cost = 0.0007 / 1e+9  # dollars per byte
        self.scan_cost = 0.002 / 1e+9  # dollars per byte

    def compute_block(self, data_scanned: int, data_returned: int, files_requested: int) -> float:
        """
        Creates an estimate of the cost for a single file.
        """
        data_returned_cost = self.returned_cost * data_returned
        data_request_cost = self.returned_cost * files_requested
        data_scanned_cost = self.scan_cost * data_scanned

        total_cost = data_returned_cost + data_request_cost + data_scanned_cost

        return total_cost

    @staticmethod
    def compute_stack(cost_values: list) -> float:
        return float(f'{sum(cost_values):.9f}')
