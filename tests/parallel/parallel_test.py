from select_plus import SSP
from select_plus.serializers import CompressionTypes, InputSerialization, OutputSerialization, CSVInputSerialization, CSVOutputSerialization


bucket_name = 's3selectplus'

ssp = SSP(
    bucket_name=bucket_name,
    prefix='json/0',
    verbose=True
)


def transform(response):
    return response


if __name__ == '__main__':
    result = ssp.select(
        threads=8,
        sql_query='SELECT * FROM s3object[*][*] s',
        extra_func=transform,
    )

    print(result.payload_dict)

