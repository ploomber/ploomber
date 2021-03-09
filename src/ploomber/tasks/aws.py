import logging

from ploomber.tasks.abc import Task
from ploomber.sources import FileSource
from ploomber.util import requires


class UploadToS3(Task):
    """
    Upload a file to S3

    Parameters
    ----------
    source: str
        Path to file to upload
    product: ploomber.products.GenericProduct
        Product must be initialized with the desired Key
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    bucket: str
        Bucked to upload
    """
    def __init__(self,
                 source,
                 product,
                 dag,
                 bucket,
                 name=None,
                 params=None,
                 client_kwargs=None,
                 upload_file_kwargs=None):
        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(source, kwargs)
        super().__init__(product, dag, name, params)
        self._bucket = bucket
        self._client_kwargs = client_kwargs
        self._upload_file_kwargs = upload_file_kwargs

    @requires(['boto3'], 'UploadToS3')
    def run(self):
        import boto3
        from botocore.exceptions import ClientError

        client_kwargs = self._client_kwargs or {}
        upload_file_kwargs = self._upload_file_kwargs or {}
        s3_client = boto3.client('s3', **client_kwargs)
        source = str(self.source)

        try:
            s3_client.upload_file(source, self._bucket, str(self.product),
                                  upload_file_kwargs)
        except ClientError as e:
            logging.error(e)

    @staticmethod
    def _init_source(source, kwargs):
        return FileSource(source, **kwargs)
