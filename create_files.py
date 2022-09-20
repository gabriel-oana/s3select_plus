from select_plus.src.aws.s3 import S3


s3 = S3()
bucket_name = 's3rdb-temp'

prefix = 1
for i in range(100):
    body = {"file": i}

    if i % 10 == 0:
        prefix = i
    key = f'json/{prefix}/file{i}.json'
    print(key)
    s3.put_object(bucket_name=bucket_name, key=key, body=body)
