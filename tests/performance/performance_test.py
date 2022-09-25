# Performance testing works on different cases:

# Case      |       Engine      |       File Type       |       Files       |       Total Size      |       Output      |       Columns Selected
# ------------------------------------------------------------------------------------------------------------------------------------------------
# 1         |       Sequential  |       json            |       200         |       100 MB          |       json        |       1
# 2         |       Parallel    |       json            |       200         |       100 MB          |       json        |       1
# 3         |       Sequential  |       json            |       200         |       100 MB          |       json        |       5
# 4         |       Parallel    |       json            |       200         |       100 MB          |       json        |       5
# 5         |       Sequential  |       csv             |       100         |       100 MB          |       csv         |       1
# 6         |       Parallel    |       csv             |       100         |       100 MB          |       csv         |       1
# 7         |       Sequential  |       csv             |       100         |       100 MB          |       csv         |       5
# 8         |       Parallel    |       csv             |       100         |       100 MB          |       csv         |       5
# 9         |       Sequential  |       parquet         |       100         |       100 MB          |       json        |       1
# 10        |       Parallel    |       parquet         |       100         |       100 MB          |       json        |       1
# 11        |       Sequential  |       parquet         |       100         |       100 MB          |       json        |       5
# 12        |       Parallel    |       parquet         |       100         |       100 MB          |       json        |       5
# 13        |       Sequential  |       parquet         |       100         |       100 MB          |       csv         |       1
# 14        |       Parallel    |       parquet         |       100         |       100 MB          |       csv         |       1
# 15        |       Sequential  |       parquet         |       100         |       100 MB          |       csv         |       5
# 16        |       Parallel    |       parquet         |       100         |       100 MB          |       csv         |       5

import time
from tabulate import tabulate
from select_plus import SSP, SequentialEngine, ParallelEngine
from select_plus.serializers import InputSerialization, OutputSerialization, JSONInputSerialization, JSONOutputSerialization,\
    CSVInputSerialization, CSVOutputSerialization


class PerformanceTest:

    def __init__(self, bucket_name: str = 's3selectplus', threads: int = 32):
        self.bucket_name = bucket_name
        self.csv_prefix = 'csv'
        self.json_prefix = 'json'
        self.parquet_prefix = 'parquet'
        self.threads = threads
        self.results = []

    def __call__(self):
        self.case1()
        self.case2()
        self.case3()
        self.case4()
        self.case5()
        self.case6()
        self.case7()
        self.case8()
        self.case9()
        self.case10()
        self.case11()
        self.case12()

        self._tabulate_results()

    def _tabulate_results(self):
        results = tabulate(
            tabular_data=self.results,
            headers=['case', 'engine', 'file_format', 'files', 'total_size', 'columns_selected', 'responses',  'time_taken_sec', 'cost'],
            tablefmt="orgtbl"
        )
        with open('results.txt', 'w') as f:
            f.write(results)

        print(results)

    def case1(self):
        print('Performance Test - Case 1')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=SequentialEngine,
            prefix=self.json_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT s.id FROM s3object[*][*] s',
            input_serialization=InputSerialization(
                json=JSONInputSerialization(
                    Type='DOCUMENT'
                )
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append(
            [1, 'sequential', 'json', 200, '100 MB', 1, responses, time_spent, response.stats.cost]
        )

    def case2(self):
        print('Performance Test - Case 2')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=ParallelEngine,
            prefix=self.json_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT s.id FROM s3object[*][*] s',
            input_serialization=InputSerialization(
                json=JSONInputSerialization(
                    Type='DOCUMENT'
                )
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([2, 'parallel', 'json', 200, '100 MB', 1, responses, time_spent, response.stats.cost])

    def case3(self):
        print('Performance Test - Case 3')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=SequentialEngine,
            prefix=self.json_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT s.id, s.ts, s.ticker, s.dt, s.updated_at  FROM s3object[*][*] s',
            input_serialization=InputSerialization(
                json=JSONInputSerialization(
                    Type='DOCUMENT'
                )
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([3, 'sequential', 'json', 200, '100 MB', 5, responses, time_spent, response.stats.cost])

    def case4(self):
        print('Performance Test - Case 4')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=ParallelEngine,
            prefix=self.json_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT s.id, s.ts, s.ticker, s.dt, s.updated_at  FROM s3object[*][*] s',
            input_serialization=InputSerialization(
                json=JSONInputSerialization(
                    Type='DOCUMENT'
                )
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([4, 'parallel', 'json', 200, '100 MB', 5, responses, time_spent, response.stats.cost])

    def case5(self):
        print('Performance Test - Case 5')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=SequentialEngine,
            prefix=self.csv_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT _1 FROM s3object s',
            input_serialization=InputSerialization(
                csv=CSVInputSerialization()
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([5, 'sequential', 'csv', 100, '100 MB', 1, responses, time_spent, response.stats.cost])

    def case6(self):
        print('Performance Test - Case 6')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=ParallelEngine,
            prefix=self.csv_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT _1 FROM s3object s',
            input_serialization=InputSerialization(
                csv=CSVInputSerialization()
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([6, 'parallel', 'csv', 100, '100 MB', 1, responses, time_spent, response.stats.cost])

    def case7(self):
        print('Performance Test - Case 7')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=SequentialEngine,
            prefix=self.csv_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT _1,_2,_3,_4,_5 FROM s3object s',
            input_serialization=InputSerialization(
                csv=CSVInputSerialization()
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([7, 'sequential', 'csv', 100, '100 MB', 5, responses, time_spent, response.stats.cost])

    def case8(self):
        print('Performance Test - Case 8')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=ParallelEngine,
            prefix=self.csv_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT _1,_2,_3,_4,_5 FROM s3object s',
            input_serialization=InputSerialization(
                csv=CSVInputSerialization()
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([8, 'parallel', 'csv', 100, '100 MB', 5, responses, time_spent, response.stats.cost])

    def case9(self):
        print('Performance Test - Case 9')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=SequentialEngine,
            prefix=self.parquet_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT s.first_name FROM s3object s',
            input_serialization=InputSerialization(
                parquet={}
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([9, 'sequential', 'parquet', 100, '100 MB', 1, responses, time_spent, response.stats.cost])

    def case10(self):
        print('Performance Test - Case 10')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=ParallelEngine,
            prefix=self.parquet_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT s.first_name FROM s3object s',
            input_serialization=InputSerialization(
                parquet={}
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([10, 'parallel', 'parquet', 100, '100 MB', 1, responses, time_spent, response.stats.cost])

    def case11(self):
        print('Performance Test - Case 11')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=SequentialEngine,
            prefix=self.parquet_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT s.first_name, s.last_name, s.university, s.title, s.sex FROM s3object s',
            input_serialization=InputSerialization(
                parquet={}
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([11, 'sequential', 'parquet', 100, '100 MB', 5, responses, time_spent, response.stats.cost])

    def case12(self):
        print('Performance Test - Case 12')
        start = time.time()
        ssp = SSP(
            bucket_name=self.bucket_name,
            engine=ParallelEngine,
            prefix=self.parquet_prefix,
            verbose=True
        )

        response = ssp.select(
            threads=self.threads,
            sql_query='SELECT s.first_name, s.last_name, s.university, s.title, s.sex FROM s3object s',
            input_serialization=InputSerialization(
                parquet={}
            ),
            output_serialization=OutputSerialization(
                json=JSONOutputSerialization()
            )
        )

        responses = len(response.payload)
        end = time.time()
        time_spent = round(end - start, 2)

        self.results.append([12, 'parallel', 'parquet', 100, '100 MB', 5, responses, time_spent, response.stats.cost])


if __name__ == '__main__':
    p = PerformanceTest()
    p()






