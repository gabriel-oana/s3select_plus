import boto3
from typing import Optional, Union

from abc import ABC, abstractmethod
from select_plus.src.aws.s3 import S3
from select_plus.src.models.models import InputSerialization, OutputSerialization


class BaseEngine(ABC):

    def __init__(self, bucket_name: str, prefix: str, threads: int, verbose: bool):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.verbose = verbose
        self.threads = threads

    @abstractmethod
    def execute(self,
                sql_query: str,
                input_serialization: Union[InputSerialization, dict],
                output_serialization: Union[OutputSerialization, dict],
                extra_func: Optional[callable] = None,
                extra_func_args: Optional[dict] = None,
                s3_client: Optional[boto3.session.Session.client] = None
                ):
        raise NotImplementedError

    def select_s3(self,
                  key: str,
                  sql_query: str,
                  input_serialization: dict,
                  output_serialization: dict,
                  extra_func: callable,
                  extra_func_args: dict,
                  s3_client: Optional[boto3.session.Session.client] = None
                  ):
        s3 = S3(client=s3_client)
        response = s3.select(bucket_name=self.bucket_name, key=key, sql_string=sql_query,
                             input_serialization=input_serialization, output_serialization=output_serialization)
        if extra_func:
            response = self._apply_extra_func(response, extra_func, extra_func_args)

        return response

    @staticmethod
    def _apply_extra_func(response: dict, extra_func, extra_func_args):
        """
        A user has the possibility of adding an additional function at each thread level to process each chunk of data
        before it merges the results from all threads.
        Allow the function to access only the payload but not the statistics.
        This way, the cost can be computed in the compilation of the results after the proceses have ended.
        """

        block_response = {
            "stats": response['stats'],
            "payload": None
        }

        func_response = extra_func(response['payload'], **extra_func_args)
        block_response['payload'] = func_response
        return block_response
