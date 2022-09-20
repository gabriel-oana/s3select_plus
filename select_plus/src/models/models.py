from dataclasses import dataclass


@dataclass
class EngineResultsStats:
    cost: float
    files_processed: int
    bytes_scanned: int
    bytes_returned: int
    bytes_processed: int


@dataclass
class EngineResults:
    payload: list
    stats: EngineResultsStats
