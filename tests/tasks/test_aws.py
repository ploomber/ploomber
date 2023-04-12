from pathlib import Path

import pytest
import boto3
from moto import mock_s3

from ploomber.tasks import UploadToS3
from ploomber.products import GenericProduct
from ploomber.clients import SQLAlchemyClient
from ploomber import DAG


@pytest.fixture(scope="function")
def s3():
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="some-bucket")
        yield client


def test_upload_to_s3(s3, tmp_directory):
    dag = DAG()
    dag.clients[GenericProduct] = SQLAlchemyClient("sqlite://")
    Path("somefile.txt").touch()
    UploadToS3(
        "somefile.txt",
        GenericProduct("somefile-in-s3.txt"),
        dag,
        bucket="some-bucket",
        name="s3_upload",
    )

    dag.build()

    contents = s3.list_objects(Bucket="some-bucket")["Contents"]

    assert len(contents) == 1
    assert contents[0]["Key"] == "somefile-in-s3.txt"
