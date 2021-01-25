from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber.clients import GCloudStorageClient
from ploomber.clients.gcloud import storage


@pytest.fixture
def mock_client(monkeypatch):
    mock_client = Mock()
    monkeypatch.setattr(storage, 'Client', lambda: mock_client)
    return mock_client


def test_upload(tmp_directory, mock_client):
    Path('source.txt').touch()
    client = GCloudStorageClient('my-bucket-name')

    client.upload('source.txt', 'destiny.txt')

    mock_client.bucket.assert_called_once_with('my-bucket-name')
    mock_client.bucket().blob.assert_called_once_with('destiny.txt')
    mock_client.bucket().blob().upload_from_filename.assert_called_once_with(
        'source.txt')


def test_download(tmp_directory, mock_client):
    Path('source.txt').touch()
    client = GCloudStorageClient('my-bucket-name')

    client.download('source.txt', 'destiny.txt')

    mock_client.bucket.assert_called_once_with('my-bucket-name')
    mock_client.bucket().blob.assert_called_once_with('source.txt')
    mock_client.bucket().blob().download_to_filename.assert_called_once_with(
        'destiny.txt')
