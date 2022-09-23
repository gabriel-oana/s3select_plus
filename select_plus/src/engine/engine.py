from typing import Optional, Union

from select_plus.src.engine.base_engine import BaseEngine
from select_plus.src.utils.cost import Cost
from select_plus.src.models.models import EngineResults, EngineResultsStats, InputSerialization, OutputSerialization


class EngineWrapper:

    def execute(
            self,
            sql_query: str,
            extra_func: Optional[callable],
            extra_func_args: Optional[dict],
            engine: BaseEngine,
            input_serialization: Union[InputSerialization, dict],
            output_serialization: Union[OutputSerialization, dict]
    ) -> EngineResults:

        dict_input_serialization = self.deserialize(input_serialization)
        dict_output_serialization = self.deserialize(output_serialization)

        response = engine.execute(
            sql_query=sql_query,
            extra_func=extra_func,
            extra_func_args=extra_func_args,
            input_serialization=dict_input_serialization,
            output_serialization=dict_output_serialization
        )
        compiled_result = self._compile_results(response)

        return compiled_result

    @staticmethod
    def deserialize(obj) -> dict:
        """
        To allow users to have the serializers as either a defined class or a dictionary, this is required to convert
        the input / output serializers into dicts to be passed to the S3 select function
        """
        if isinstance(obj, OutputSerialization):
            return obj.as_dict()
        elif isinstance(obj, InputSerialization):
            return obj.as_dict()
        else:
            return obj

    @staticmethod
    def _compile_results(response: list) -> EngineResults:
        cost = Cost()

        payload = []
        bytes_scanned = 0
        bytes_processed = 0
        bytes_returned = 0
        files_processed = 0

        for r in response:
            payload.append(r['payload'])
            bytes_scanned += r['stats']['bytes_scanned']
            bytes_processed += r['stats']['bytes_processed']
            bytes_returned += r['stats']['bytes_returned']
            files_processed += 1

        cost = cost.compute_block(data_scanned=bytes_scanned,
                                  data_returned=bytes_returned,
                                  files_requested=files_processed)

        model = EngineResults(
            payload=payload,
            stats=EngineResultsStats(
                cost=cost,
                files_processed=files_processed,
                bytes_scanned=bytes_scanned,
                bytes_returned=bytes_returned,
                bytes_processed=bytes_processed
            )
        )

        return model
