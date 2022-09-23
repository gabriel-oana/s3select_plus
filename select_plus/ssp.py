from typing import Type, Union
from multiprocessing import cpu_count

from select_plus.src.aws.s3 import S3
from select_plus.src.utils.cost import Cost
from select_plus.src.models.models import EngineResults, InputSerialization, OutputSerialization, \
    JSONInputSerialization, JSONOutputSerialization
from select_plus.src.engine.base_engine import BaseEngine
from select_plus.src.engine.sequential_engine import SequentialEngine
from select_plus.src.engine.parallel_engine import ParallelEngine
from select_plus.src.engine.engine import EngineWrapper


class SSP:

    def __init__(self,
                 bucket_name: str,
                 prefix: str = '',
                 engine: Union[Type[BaseEngine], ParallelEngine, SequentialEngine] = ParallelEngine,
                 verbose: bool = False):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.verbose = verbose
        self.engine = engine

        # Validate the engine is inheriting the BaseEngine
        if not issubclass(engine, BaseEngine):
            raise RuntimeError(f'Engine {engine.__name__} does not inherit BaseEngine')

        self.cost = Cost()

    def estimate_cost(self) -> float:
        """
        Estimates the cost of selecting from all files
        """
        s3 = S3()
        s3_response = s3.list_objects(bucket_name=self.bucket_name, prefix=self.prefix)
        estimate_cost = self.cost.compute_block(
            data_scanned=s3_response['total_file_size'],
            data_returned=s3_response['total_file_size'],
            files_requested=s3_response['total_files']
        )
        return estimate_cost

    def select(
            self,
            sql_query: str,
            extra_func: callable = None,
            extra_func_args: dict = None,
            threads: int = cpu_count(),
            input_serialization: Union[InputSerialization, dict] = InputSerialization(
                json=JSONInputSerialization(Type='DOCUMENT')
            ),
            output_serialization: Union[OutputSerialization, dict] = OutputSerialization(
                json=JSONOutputSerialization()
            )
    ) -> EngineResults:
        eng = self.engine(bucket_name=self.bucket_name,
                          prefix=self.prefix,
                          threads=threads,
                          verbose=self.verbose)

        eng_wrapper = EngineWrapper()

        results = eng_wrapper.execute(sql_query=sql_query,
                                      extra_func=extra_func,
                                      extra_func_args=extra_func_args,
                                      engine=eng,
                                      input_serialization=input_serialization,
                                      output_serialization=output_serialization)

        return results
