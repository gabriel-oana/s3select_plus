import pandas as pd
from select_plus.src.aws.s3 import S3
from select_plus import SSP, SequentialEngine, ParallelEngine
from select_plus.src.models.models import InputSerialization, OutputSerialization, CSVOutputSerialization, \
    CSVInputSerialization

s3 = S3()
bucket_name = 's3selectplus'


def json_etl(payload, message, message1):
    response = eval(payload.replace('\n', ''))
    response['message'] = message
    response['message_1'] = message1
    return response


def csv_etl(payload):
    return payload


def create_files():
    prefix = 1
    for i in range(100):

        if i % 10 == 0:
            prefix = i

        # Create json files
        json_body = {"file": i}
        json_key = f'json/{prefix}/file{i}.json'

        # Create csv files
        with open('tests/files/sample.csv', 'r') as f:
            csv_body = f.read()
        csv_key = f'csv/{prefix}/file{i}.csv'

        # Create parquet files
        df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
        df.to_parquet(f's3://{bucket_name}/parquet/{prefix}/file{i}.parquet', index=False, compression='snappy')

        print(i)
        s3.put_object(bucket_name=bucket_name, key=json_key, body=json_body)
        s3.put_object(bucket_name=bucket_name, key=csv_key, body=csv_body)


def json_sequential():
    ssp = SSP(
        bucket_name='s3rdb-temp',
        engine=ParallelEngine,
        prefix='json/90',
        verbose=True
    )
    est_cost = ssp.estimate_cost()
    print(f'Estimated Cost: ${format(est_cost, "f")}')

    reply = ssp.select(
        threads=32,
        sql_query='SELECT * FROM s3object s',
        extra_func=json_etl,
        extra_func_args={"message": "appended", "message1": 1}
    )
    print(reply)


def json_parallel():
    ssp = SSP(
        bucket_name='s3rdb-temp',
        engine=SequentialEngine,
        prefix='json/90',
        verbose=True
    )
    est_cost = ssp.estimate_cost()
    print(f'Estimated Cost: ${format(est_cost, "f")}')

    reply = ssp.select(
        threads=32,
        sql_query='SELECT * FROM s3object s',
        extra_func=json_etl,
        extra_func_args={"message": "appended", "message1": 1}
    )
    print(reply)


def csv_sequential():
    ssp = SSP(
        bucket_name='s3rdb-temp',
        engine=SequentialEngine,
        prefix='csv/90',
        verbose=True
    )
    est_cost = ssp.estimate_cost()
    print(f'Estimated Cost: ${format(est_cost, "f")}')

    reply = ssp.select(
        threads=32,
        sql_query='SELECT * FROM s3object s',
        input_serialization=InputSerialization(
            csv=CSVInputSerialization()
        ),
        output_serialization=OutputSerialization(
            csv=CSVOutputSerialization()
        ),
        extra_func=csv_etl,
        extra_func_args={}
    )
    print(reply)


def csv_parallel():
    ssp = SSP(
        bucket_name='s3rdb-temp',
        engine=ParallelEngine,
        prefix='csv/90',
        verbose=True
    )
    est_cost = ssp.estimate_cost()
    print(f'Estimated Cost: ${format(est_cost, "f")}')

    reply = ssp.select(
        threads=32,
        sql_query='SELECT * FROM s3object s',
        input_serialization=InputSerialization(
            csv=CSVInputSerialization()
        ),
        output_serialization=OutputSerialization(
            csv=CSVOutputSerialization()
        ),
        extra_func=csv_etl,
        extra_func_args={}
    )
    print(reply)


def parquet_sequential():
    ssp = SSP(
        bucket_name='s3rdb-temp',
        engine=SequentialEngine,
        prefix='parquet/90',
        verbose=True
    )
    est_cost = ssp.estimate_cost()
    print(f'Estimated Cost: ${format(est_cost, "f")}')

    reply = ssp.select(
        threads=32,
        sql_query='SELECT * FROM s3object s',
        input_serialization=InputSerialization(
            parquet={}
        ),
        output_serialization=OutputSerialization(
            csv=CSVOutputSerialization()
        ),
        extra_func=csv_etl,
        extra_func_args={}
    )
    print(reply)


def parquet_parallel():
    ssp = SSP(
        bucket_name='s3rdb-temp',
        engine=ParallelEngine,
        prefix='parquet/90',
        verbose=True
    )
    est_cost = ssp.estimate_cost()
    print(f'Estimated Cost: ${format(est_cost, "f")}')

    reply = ssp.select(
        threads=32,
        sql_query='SELECT * FROM s3object s',
        input_serialization=InputSerialization(
            parquet={}
        ),
        output_serialization=OutputSerialization(
            csv=CSVOutputSerialization()
        ),
        extra_func=csv_etl,
        extra_func_args={}
    )
    print(reply)


if __name__ == '__main__':
    # create_files()
    # json_sequential()
    # json_parallel()
    # csv_sequential()
    # csv_parallel()
    # parquet_sequential()
    parquet_parallel()



