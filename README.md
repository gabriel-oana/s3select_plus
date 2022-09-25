<span><img src="https://img.shields.io/github/workflow/status/gabriel-oana/s3select_plus/Tests">
<img src="https://img.shields.io/github/languages/top/gabriel-oana/s3select_plus">
<img src="https://img.shields.io/pypi/pyversions/s3select-plus">
<img src="https://img.shields.io/pypi/v/s3select-plus">
<img src="https://img.shields.io/badge/linting-pylint-green">
<img src="https://img.shields.io/github/downloads/gabriel-oana/s3select_plus/total">
<img src="https://img.shields.io/pypi/dm/s3select_plus?label=pypi%20downloads">
[![codecov](https://codecov.io/gh/gabriel-oana/s3select_plus/branch/test/badge/graph/badge.svg?token=5BW32QG1KJ)](https://codecov.io/gh/gabriel-oana/s3select_plus)
</span>


# S3 Select Plus

- [S3 Select Plus](#s3-select-plus)
    + [1. Description](#1-description)
    + [2 Features](#2-features)
    + [3. Installation](#3-installation)
    + [4. Usage](#4-usage)
      - [4.1 Basic](#41-basic)
      - [4.2 Running with an "extra function"](#42-running-with-an--extra-function-)
      - [4.3 Running with SequentialEngine](#43-running-with-sequentialengine)
      - [4.4 Show statistics](#44-show-statistics)
      - [4.4 Serialization](#44-serialization)
        * [4.4.1 JSON Serialization](#441-json-serialization)
        * [4.4.2 CSV Serialization](#442-csv-serialization)
        * [4.4.3 Parquet Serialization](#443-parquet-serialization)
        * [4.4.4 Indirect Serialization](#444-indirect-serialization)
    + [5. Development](#5-development)
      - [5.1 Creating a parallel engine with a different S3 client implementation](#51-creating-a-parallel-engine-with-a-different-s3-client-implementation)

### 1. Description
Utility package to query multiple S3 objects using S3 Select.
More information on AWS S3 Select: https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html

### 2 Features
* Engine modes:
  * Parallel - each file gets queries in a separate process
  * Sequential - all files get queried sequentially
  * User defined query engine (more on this below)
* Cost estimation (before query) and calculation (after query)
* Possibility to add user defined functions at process level (useful for in-flight transformations)
* Support for formats: JSON, CSV and Parquet files
* Support for compressions: GZIP, BZIP
* Support for Input and Output Serialization using SerializerTypes of dictionary config. 
* Support for user defined SQL Query

#### 2.1 Future versions:
* Scan Range functionality exposed
* SSE functionality exposed
* Ability to select profile for S3 connections

#### 2.2 Performance
The parallelisation of the file query allows one to get to time to query multiple files much faster. 
The performance test below has been performed.

For many small files of different types the following results have been achieved on an 8 core CPU (network transfer speeds ignored):
```text
|   case | engine     | file_type   |   files | total_size   |   columns |   responses |   time_taken_sec |        cost |
|--------+------------+-------------+---------+--------------+-----------+-------------+------------------+-------------|
|      1 | sequential | json        |     200 | 100 MB       |         1 |         200 |            42.53 | 0.000232982 |
|      2 | parallel   | json        |     200 | 100 MB       |         1 |         200 |             5.79 | 0.000232982 |
|      3 | sequential | json        |     200 | 100 MB       |         5 |         200 |            44.71 | 0.000256532 |
|      4 | parallel   | json        |     200 | 100 MB       |         5 |         200 |             6.81 | 0.000256532 |
|      5 | sequential | csv         |     100 | 100 MB       |         1 |         100 |            26.54 | 0.000247681 |
|      6 | parallel   | csv         |     100 | 100 MB       |         1 |         100 |             7.14 | 0.000247681 |
|      7 | sequential | csv         |     100 | 100 MB       |         5 |         100 |            38.26 | 0.000373264 |
|      8 | parallel   | csv         |     100 | 100 MB       |         5 |         100 |            16.2  | 0.000373264 |
|      9 | sequential | parquet     |     100 | 100 MB       |         1 |         100 |            43.79 | 7.39638e-05 |
|     10 | parallel   | parquet     |     100 | 100 MB       |         1 |         100 |            21.05 | 7.39638e-05 |
|     11 | sequential | parquet     |     100 | 100 MB       |         5 |         100 |            64.34 | 0.00034574  |
|     12 | parallel   | parquet     |     100 | 100 MB       |         5 |         100 |            28.34 | 0.00034574  |
|     13 | sequential | json        |    2000 | 1 GB         |         1 |        2000 |           552.23 | 0.00232982  |
|     14 | parallel   | json        |    2000 | 1 GB         |         1 |        2000 |           330.23 | 0.00232982  |
```


### 3. Installation

```shell
pip3 install s3select_plus
```

### 4. Usage

#### 4.1 Basic
By default, this is a parallel process querying JSON files.

```python
from select_plus import SSP


ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix'
)

est_cost = ssp.estimate_cost()
print(f'Estimated Cost: ${format(est_cost, "f")}')

# The line below must be added when executing using the ParallelEngine.
# Reason for this is that each file will be queried into a different process.
# The "multiprocessing" python package is restricted to this.

if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s'
    )
    
    print(result.payload)
```

#### 4.2 Running with an "extra function"
The "extra function" can be defined to do extra steps for each result from a single SQL query
in a process. For example, if one needs to do some processing or transformation of the results
before all the results are combined into the final result.
The "extra function" also supports "extra function arguments" to be passed to the function.

```python
from select_plus import SSP


ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix'
)

def transform(response, arg1, arg2):
    # Assuming the response from the query looks like: {"column1": 1}
    # response = {"column": 1}
    response['new_column'] = arg1
    response['newer_column'] = arg2
    
    # This function must always return something
    return response
    

if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s',
        extra_func=transform,
        extra_func_args={
          "arg1": 1,
          "arg2": 2
        }
    )
    
    print(result.payload)
```

#### 4.3 Running with SequentialEngine
```python
from select_plus import SSP, SequentialEngine


ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix',
    engine=SequentialEngine
)

if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s'
    )
    
    print(result.payload)
```

#### 4.4 Show statistics
```python
from select_plus import SSP, ParallelEngine


ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix',
    engine=ParallelEngine
)

if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s'
    )
    
    print(result.payload)
    print(result.stats.cost) # dollars
    print(result.stats.bytes_processed)
    print(result.stats.bytes_returned)
    print(result.stats.bytes_scanned)
    print(result.stats.files_processed)
```

#### 4.4 Serialization
The ```select_object_content``` S3 API call will expect some serialization parameters depending on the types of files that are queried.      
Serialization works by default with all engine types.       
A full list of all the parameters accessible for the serializers is better described here: 
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.select_object_content

##### 4.4.1 JSON Serialization
To query JSON files with specific types and parameters one can use the pre-defined Serialization objects
```python
from select_plus import SSP
from select_plus.serializers import CompressionTypes, InputSerialization, OutputSerialization, JSONInputSerialization, JSONOutputSerialization

ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix'
)

if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s',
        input_serialization=InputSerialization(
            compression_type=CompressionTypes.gzip,
            json=JSONInputSerialization(
                Type='DOCUMENT'
            )
        ),
        output_serialization=OutputSerialization(
            json=JSONOutputSerialization(
                record_delimiter='\n'
            )
        )
    )
    
    print(result.payload)

```

##### 4.4.2 CSV Serialization
To query CSV files with specific types and parameters one can use the pre-defined Serialization objects
```python
from select_plus import SSP
from select_plus.serializers import CompressionTypes, InputSerialization, OutputSerialization, CSVInputSerialization, CSVOutputSerialization

ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix'
)

if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s',
        input_serialization=InputSerialization(
            compression_type=CompressionTypes.gzip,
            csv=CSVInputSerialization(
                file_header_info='USE'
            )
        ),
        output_serialization=OutputSerialization(
            csv=CSVOutputSerialization(
                record_delimiter='\n'
            )
        )
    )
    
    print(result.payload)

```

##### 4.4.3 Parquet Serialization
To query parquet files with specific types and parameters one can use the pre-defined Serialization objects
```python
from select_plus import SSP
from select_plus.serializers import CompressionTypes, InputSerialization, OutputSerialization, CSVOutputSerialization

ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix'
)

if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s',
        input_serialization=InputSerialization(
            compression_type=CompressionTypes.gzip,
            parquet={}
        ),
        output_serialization=OutputSerialization(
            csv=CSVOutputSerialization(
                record_delimiter='\n'
            )
        )
    )
    
    print(result.payload)

```

##### 4.4.4 Indirect Serialization
One can use a simple JSON format to input the serialization types if the Serialization objects should be a part of a configuration file for example.
```python
from select_plus import SSP

ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix'
)

if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s',
        input_serialization={
            "CompressionType": 'GZIP',
            "JSON": {
                "Type": "LINES"
            }
        },
        output_serialization={
            "JSON": {
                'RecordDelimiter': '\n'
            }
        }
    )
    
    print(result.payload)

```


### 5. Development
#### 5.1 Creating a parallel engine with a different S3 client implementation
One downside to this package is that the S3 client cannot be treated as an input into the main call.
The reason is that each individual S3 client must be initialised once per process (restricted by AWS) and cannot be pickled.
To circumvent this problem, one can create their own engine where they can implement their own S3 client (or resource).

```python
from select_plus import SSP, BaseEngine
from select_plus.src.aws.s3 import S3


class MyCustomEngine(BaseEngine):
  
    def execute(self, sql_query: str, extra_func: callable = None, extra_func_args: dict = None) -> list:
        pass
    
    def _make_func_args(self, sql_query: str, extra_func: callable, extra_func_args: dict):
        """
        Gets a list of all keys to be processed
        """
        s3 = S3(client='my-custom-client') # This is where you can customize your own S3 client. Even change the entire S3 functionality.
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
        s3 = S3() # This is where you can customize your own S3 client. Even change the entire S3 functionality.
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

    
ssp = SSP(
    bucket_name='bucket-name',
    prefix='s3-key-prefix',
    engine=MyCustomEngine
)


if __name__ == '__main__':
    
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*] s'
    )
    
    print(result.payload)

```

Similarly, one can develop new engines. For example using Dask or PySpark.