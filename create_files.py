
from select_plus.src.aws.s3 import S3
from select_plus.src.engine.parallel_engine import ParallelEngine


s3 = S3()
bucket_name = 's3selectplus'
with open('tests/files/sample.json', 'r') as j:
    json_body = j.read()


def create_one_json_file(i):
    json_key = f'large_json/file{i}.json'
    s3.put_object(bucket_name=bucket_name, key=json_key, body=json_body)


def create_json_files_parallel():
    p = ParallelEngine(
        bucket_name=bucket_name,
        prefix='large_json',
        threads=32,
        verbose=True
    )
    func_args = list(range(0, 2000))
    # print(func_args)
    p.execute_callable(create_one_json_file, func_args)


def create_files():

    prefix = 1
    for i in range(2000):
        print(i)
        if i % 100 == 0:
            prefix = i

        # Create json files
        with open('tests/files/sample.json', 'r') as j:
            json_body = j.read()
        json_key = f'large_json/{prefix}/file{i}.json'
        s3.put_object(bucket_name=bucket_name, key=json_key, body=json_body)


def create_tabular_files():
    import farsante
    from mimesis import Person
    prefix = 1
    for i in range(200):
        print(i)
        if i % 10 == 0:
            prefix = i

        # Create csv files
        with open('tests/files/sample.csv', 'r') as f:
            csv_body = f.read()
        csv_key = f'csv/{prefix}/file{i}.csv'

        # Create large parquet files
        mx = Person('en')
        df = farsante.pandas_df([mx.first_name, mx.last_name, mx.university, mx.title, mx.views_on, mx.weight, mx.sex, mx.political_views, mx.occupation, mx.nationality], 26000)
        df.to_parquet(f's3://{bucket_name}/parquet/{prefix}/file{i}.parquet', index=False, compression='snappy')

        s3.put_object(bucket_name=bucket_name, key=csv_key, body=csv_body)


if __name__ == '__main__':
    create_json_files_parallel()


