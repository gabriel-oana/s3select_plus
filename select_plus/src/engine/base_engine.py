from abc import ABC, abstractmethod


class BaseEngine(ABC):

    def __init__(self, bucket_name: str, prefix: str, threads: int, verbose: bool):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.verbose = verbose
        self.threads = threads

    @abstractmethod
    def execute(self, sql_query: str, extra_func: callable = None, extra_func_args: dict = None):
        raise NotImplementedError
