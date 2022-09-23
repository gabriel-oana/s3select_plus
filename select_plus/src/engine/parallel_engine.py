import tqdm
import boto3
from typing import Optional
from multiprocessing import Pool
from select_plus.src.engine.base_engine import BaseEngine
from select_plus.src.aws.s3 import S3


class ParallelEngine(BaseEngine):

    def execute(self,
                sql_query: str,
                extra_func: Optional[callable] = None,
                extra_func_args: Optional[dict] = None,
                s3_client: Optional[boto3.session.Session.client] = None) -> list:

        function_args = self._make_func_args(sql_query=sql_query, extra_func=extra_func,
                                             extra_func_args=extra_func_args)

        results = self.execute_callable(self._wrapper_func, function_args)
        return results

    def execute_callable(self, func: callable, args: list) -> list:
        """
        Generic parallel executor for a function with a list of arguments.
        The args must be of format [(arg1, arg2, arg3...), (arg1, arg2, arg3...)]
        """
        with Pool(self.threads) as pool:
            if self.verbose:
                print(f'Running with {self.threads} processes')
                # result = pool.starmap(func, tqdm.tqdm(args, total=len(args)))
                result = list(tqdm.tqdm(pool.imap(func, args), total=len(args)))
            else:
                result = pool.imap(func, args)
            return result

    def _make_func_args(self, sql_query: str, extra_func: callable, extra_func_args: dict):
        """
        Gets a list of all keys to be processed
        """
        s3 = S3()
        s3_keys = s3.list_objects(bucket_name=self.bucket_name, prefix=self.prefix)
        keys = s3_keys['keys']
        func_args = [(key, sql_query, extra_func, extra_func_args) for key in keys]
        return func_args

    def _wrapper_func(self, args):
        """
        This is a wrapper for the function to be executed inside the thread.
        The reason why this exists, is because if the tqdm bar is to exist, then tqdm doesn't work with "pool.starmap".
        As result, one cannot pass multiple parameters to the function.
        """
        result = self.select_s3(*args)
        return result
