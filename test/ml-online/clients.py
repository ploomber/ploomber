from ploomber.clients import S3Client

def get_s3():
    return S3Client(bucket_name='api-stack-dev-ploombercloudartifactsd7322d06-1bd3rk42l1ns5',
                    parent='ml-online-output',
                    json_credentials_path='credentials.json')