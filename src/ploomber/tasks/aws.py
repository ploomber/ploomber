from pathlib import Path
import logging
import boto3
from botocore.exceptions import ClientError

from ploomber.tasks.Task import Task
from ploomber.sources import FileSource


class UploadToS3(Task):
    """
    Upload a file to S3

    Parameters
    ----------
    source: str
        Path to file to upload
    product: ploomber.products.File
        Product generated upon successful execution
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    bucket: str
        Bucked to upload
    """

    def __init__(self, source, product, dag, bucket, name=None, params=None,
                 client_kwargs=None):
        super().__init__(source, product, dag, name, params)
        self._bucket = bucket
        self._client_kwargs = client_kwargs

    def run(self):
        client_kwargs = self._client_kwargs or {}
        s3_client = boto3.client('s3', **client_kwargs)
        source = str(self.source)
        name = str(Path(source).name)
        try:
            s3_client.upload_file(source, self._bucket, name)
        except ClientError as e:
            logging.error(e)

    def _init_source(self, source):
        return FileSource(source)
