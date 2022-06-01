from ploomber.clients import LocalStorageClient, GCloudStorageClient, S3Client


def get_local():
    """Returns local client
    """
    return LocalStorageClient('backup')


def get_s3():
    """Returns S3 client
    """
    # assumes your environment is already configured, you may also pass the
    # json_credentials_path
    return S3Client(bucket_name='some-bucket', parent='my-project/products')


def get_gcloud():
    """Returns google cloud storage client
    """
    # assumes your environment is already configured, you may also pass the
    # json_credentials_path
    return GCloudStorageClient(bucket_name='some-bucket',
                               parent='my-project/products')
