import tqdm
import boto3
from typing import Optional
from select_plus.src.aws.s3 import S3
from select_plus.src.engine.base_engine import BaseEngine


class SequentialEngine(BaseEngine):

    def execute(self,
                sql_query: str,
                input_serialization: dict,
                output_serialization: dict,
                extra_func: Optional[callable] = None,
                extra_func_args: Optional[dict] = None,
                s3_client: Optional[boto3.session.Session.client] = None) -> list:
        s3 = S3(client=s3_client)
        s3_keys = s3.list_objects(bucket_name=self.bucket_name, prefix=self.prefix)
        keys = s3_keys['keys']

        result = []
        if self.verbose:
            itt = tqdm.tqdm(keys)
        else:
            itt = keys

        for key in itt:
            response = self.select_s3(key=key, sql_query=sql_query, extra_func=extra_func,
                                      extra_func_args=extra_func_args, s3_client=s3_client,
                                      input_serialization=input_serialization,
                                      output_serialization=output_serialization
                                      )
            result.append(response)

        return result


