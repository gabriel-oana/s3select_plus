import tqdm
from multiprocessing import Pool
from select_plus.src.engine.base_engine import BaseEngine
from select_plus.src.aws.s3 import S3


class ParallelEngine(BaseEngine):

    def execute(self, sql_query: str, extra_func: callable = None, extra_func_args: dict = None) -> list:

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

    def _make_func(self, key: str, sql_query: str, extra_func: callable, extra_func_args: dict):
        """
        Performs the SQL query against one single Key.
        This process runs as a single process.
        As result, all boto initialization must happen inside this function.
        It also applies any extra functions added by the user.
        """
        s3 = S3()
        response = s3.select(bucket_name=self.bucket_name, key=key, sql_string=sql_query)
        if extra_func:
            response = self._apply_extra_func(response, extra_func, extra_func_args)

        return response

    def _wrapper_func(self, args):
        """
        This is a wrapper for the function to be executed inside the thread.
        The reason why this exists, is because if the tqdm bar is to exist, then tqdm doesn't work with "pool.starmap".
        As result, one cannot pass multiple parameters to the function.
        """
        result = self._make_func(*args)
        return result

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
