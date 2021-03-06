import pickle
from unittest.mock import Mock
from pathlib import Path, PurePosixPath

import pytest

from ploomber.clients import GCloudStorageClient
from ploomber.clients.gcloud import storage


@pytest.fixture
def mock_client(monkeypatch):
    mock_client = Mock()
    monkeypatch.setattr(storage, 'Client', mock_client)
    return mock_client


def test_init(mock_client):
    client = GCloudStorageClient('my-bucket-name', parent='folder', arg=1)

    mock_client.assert_called_once_with(arg=1)
    mock_client().bucket.assert_called_once_with('my-bucket-name')
    assert client.parent == 'folder'
    assert client.bucket_name == 'my-bucket-name'
    assert client.bucket is mock_client().bucket()


def test_from_service_account_json(mock_client):
    client = GCloudStorageClient('my-bucket-name', 'folder', 'my.json', arg=1)

    mock_client.from_service_account_json.assert_called_once_with(
        json_credentials_path='my.json', arg=1)

    assert client.parent == 'folder'
    assert client.bucket_name == 'my-bucket-name'
    assert client.bucket is mock_client.from_service_account_json().bucket()


def test_underscore_upload(tmp_directory, mock_client):
    Path('source.txt').touch()
    client = GCloudStorageClient('my-bucket-name', parent='')

    client._upload('source.txt', 'destiny.txt')

    mock_client().bucket.assert_called_once_with('my-bucket-name')
    mock_client().bucket().blob.assert_called_once_with('destiny.txt')
    mock_client().bucket().blob().upload_from_filename.assert_called_once_with(
        'source.txt')


def test_underscore_download(tmp_directory, mock_client):
    Path('source.txt').touch()
    client = GCloudStorageClient('my-bucket-name', parent='')

    client._download('source.txt', 'destiny.txt')

    mock_client().bucket.assert_called_once_with('my-bucket-name')
    mock_client().bucket().blob.assert_called_once_with('destiny.txt')
    mock_client().bucket().blob().download_to_filename.assert_called_once_with(
        'source.txt')


@pytest.mark.parametrize('parent', ['', 'some/parent/', 'some/parent'])
def test_upload(monkeypatch, parent, mock_client):
    mock = Mock()
    client = GCloudStorageClient('my-bucket-name', parent=parent)
    monkeypatch.setattr(client, '_upload', mock)

    client.upload('file.txt')

    mock.assert_called_once_with('file.txt',
                                 str(PurePosixPath(parent, 'file.txt')))


@pytest.mark.parametrize('parent', ['', 'some/parent/', 'some/parent'])
def test_download(monkeypatch, parent, mock_client):
    mock = Mock()
    client = GCloudStorageClient('my-bucket-name', parent=parent)
    monkeypatch.setattr(client, '_download', mock)

    client.download('file.txt')

    mock.assert_called_once_with('file.txt',
                                 str(PurePosixPath(parent, 'file.txt')))


def test_pickle(mock_client):
    c = GCloudStorageClient('my-bucket-name', parent='')
    pickle.loads(pickle.dumps(c))


def test_pickle_from_service_acccount(mock_client):
    c = GCloudStorageClient('my-bucket-name', 'folder', 'my.json')
    pickle.loads(pickle.dumps(c))


def test_close(mock_client):
    GCloudStorageClient('my-bucket-name', parent='').close()
