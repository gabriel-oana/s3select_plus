import tqdm
from select_plus.src.aws.s3 import S3
from select_plus.src.engine.base_engine import BaseEngine


class SequentialEngine(BaseEngine):

    def execute(self, sql_query: str, extra_func: callable = None, extra_func_args: dict = None):
        s3 = S3()
        s3_keys = s3.list_objects(bucket_name=self.bucket_name, prefix=self.prefix)
        keys = s3_keys['keys']

        result = []
        for key in tqdm.tqdm(keys):
            response = self.select_s3(key=key, sql_query=sql_query, extra_func=extra_func,
                                      extra_func_args=extra_func_args)
            result.append(response)

        return result

    def select_s3(self, key: str, sql_query: str, extra_func: callable, extra_func_args: dict):
        s3 = S3()
        response = s3.select(bucket_name=self.bucket_name, key=key, sql_string=sql_query)
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
