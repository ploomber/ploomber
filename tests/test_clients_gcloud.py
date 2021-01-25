import pickle
from unittest.mock import Mock
from pathlib import Path, PurePosixPath

import pytest

from ploomber.clients import GCloudStorageClient
from ploomber.clients.gcloud import storage


@pytest.fixture
def mock_client(monkeypatch):
    mock_client = Mock()
    monkeypatch.setattr(storage, 'Client', lambda: mock_client)
    return mock_client


def test_underscore_upload(tmp_directory, mock_client):
    Path('source.txt').touch()
    client = GCloudStorageClient('my-bucket-name', parent='')

    client._upload('source.txt', 'destiny.txt')

    mock_client.bucket.assert_called_once_with('my-bucket-name')
    mock_client.bucket().blob.assert_called_once_with('destiny.txt')
    mock_client.bucket().blob().upload_from_filename.assert_called_once_with(
        'source.txt')


def test_underscore_download(tmp_directory, mock_client):
    Path('source.txt').touch()
    client = GCloudStorageClient('my-bucket-name', parent='')

    client._download('source.txt', 'destiny.txt')

    mock_client.bucket.assert_called_once_with('my-bucket-name')
    mock_client.bucket().blob.assert_called_once_with('destiny.txt')
    mock_client.bucket().blob().download_to_filename.assert_called_once_with(
        'source.txt')


@pytest.mark.parametrize('parent', ['', 'some/parent/', 'some/parent'])
def test_upload(monkeypatch, parent):
    mock = Mock()
    client = GCloudStorageClient('my-bucket-name', parent=parent)
    monkeypatch.setattr(client, '_upload', mock)

    client.upload('file.txt')

    mock.assert_called_once_with('file.txt',
                                 str(PurePosixPath(parent, 'file.txt')))


@pytest.mark.parametrize('parent', ['', 'some/parent/', 'some/parent'])
def test_download(monkeypatch, parent):
    mock = Mock()
    client = GCloudStorageClient('my-bucket-name', parent=parent)
    monkeypatch.setattr(client, '_download', mock)

    client.download('file.txt')

    mock.assert_called_once_with('file.txt',
                                 str(PurePosixPath(parent, 'file.txt')))


def test_pickle():
    pickle.dumps(GCloudStorageClient('my-bucket-name', parent=''))


def test_close():
    GCloudStorageClient('my-bucket-name', parent='').close()