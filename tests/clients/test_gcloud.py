import pickle
from unittest.mock import Mock, call
from pathlib import Path, PurePosixPath

import pytest

from ploomber.clients import GCloudStorageClient
from ploomber.clients.storage.gcloud import storage
from ploomber.exceptions import RemoteFileNotFound, DAGSpecInvalidError


@pytest.fixture
def mock_client(monkeypatch):
    mock_client = Mock()
    # mock gcloud Client object
    monkeypatch.setattr(storage, 'Client', mock_client)
    return mock_client


def test_init(tmp_directory, mock_client):
    client = GCloudStorageClient('my-bucket-name',
                                 parent='folder',
                                 arg=1,
                                 path_to_project_root='.')

    mock_client.assert_called_once_with(arg=1)
    mock_client().bucket.assert_called_once_with('my-bucket-name')
    assert client._parent == 'folder'
    assert client._bucket_name == 'my-bucket-name'
    assert client._bucket is mock_client().bucket()


@pytest.mark.parametrize('relative_to_project_root', [False, True])
def test_from_service_account_json(tmp_directory, mock_client,
                                   relative_to_project_root):
    client = GCloudStorageClient(
        'my-bucket-name',
        'folder',
        'my.json',
        arg=1,
        credentials_relative_to_project_root=relative_to_project_root,
        path_to_project_root='.',
    )

    expected = 'my.json' if not relative_to_project_root else Path(
        tmp_directory, 'my.json').resolve()

    mock_client.from_service_account_json.assert_called_once_with(
        json_credentials_path=expected, arg=1)

    assert client._parent == 'folder'
    assert client._bucket_name == 'my-bucket-name'
    assert client._bucket is mock_client.from_service_account_json().bucket()


def test_underscore_upload(tmp_directory, mock_client):
    Path('my-file.txt').touch()
    client = GCloudStorageClient('my-bucket-name',
                                 parent='',
                                 path_to_project_root='.')

    client._upload('my-file.txt')

    mock_client().bucket.assert_called_once_with('my-bucket-name')
    mock_client().bucket().blob.assert_called_once_with('my-file.txt')
    mock_client().bucket().blob().upload_from_filename.assert_called_once_with(
        'my-file.txt')


def test_underscore_download(tmp_directory, mock_client):
    Path('source.txt').touch()
    client = GCloudStorageClient('my-bucket-name',
                                 parent='',
                                 path_to_project_root='.')

    client._download('source.txt', 'destiny.txt')

    mock_client().bucket.assert_called_once_with('my-bucket-name')
    mock_client().bucket().blob.assert_called_once_with('destiny.txt')
    mock_client().bucket().blob().download_to_filename.assert_called_once_with(
        'source.txt')


@pytest.mark.parametrize('parent', ['', 'some/parent/', 'some/parent'])
def test_upload_file(monkeypatch, parent, mock_client):
    mock = Mock()
    client = GCloudStorageClient('my-bucket-name',
                                 parent=parent,
                                 path_to_project_root='.')
    monkeypatch.setattr(client, '_upload', mock)

    client.upload('file.txt')

    mock.assert_called_once_with('file.txt')


@pytest.mark.parametrize('parent', ['', 'some/parent/', 'some/parent'])
def test_download(monkeypatch, parent, mock_client):
    mock = Mock()
    client = GCloudStorageClient('my-bucket-name',
                                 parent=parent,
                                 path_to_project_root='.')
    monkeypatch.setattr(client, '_download', mock)

    client.download('file.txt')

    mock.assert_called_once_with('file.txt',
                                 str(PurePosixPath(parent, 'file.txt')))


def test_error_when_downloading_non_existing(monkeypatch, mock_client):
    client = GCloudStorageClient('my-bucket-name',
                                 parent='parent',
                                 path_to_project_root='.')
    monkeypatch.setattr(client, '_is_file', lambda _: False)
    list_blobs_mock = Mock(return_value=[])
    monkeypatch.setattr(client._bucket.client, 'list_blobs', list_blobs_mock)

    with pytest.raises(RemoteFileNotFound) as excinfo:
        client.download('non-existing-file')

    assert ("Could not download 'non-existing-file' using client"
            in str(excinfo.value))


def test_upload_folder(tmp_directory, monkeypatch, mock_client):
    Path('dir', 'subdir', 'nested').mkdir(parents=True)
    Path('dir', 'a').touch()
    Path('dir', 'b').touch()
    Path('dir', 'subdir', 'c').touch()
    Path('dir', 'subdir', 'nested', 'd').touch()

    mock = Mock()
    client = GCloudStorageClient('my-bucket-name',
                                 parent='.',
                                 path_to_project_root='.')
    monkeypatch.setattr(client, '_upload', mock)

    client.upload('dir')

    mock.assert_has_calls([
        call(str(Path('dir', 'a'))),
        call(str(Path('dir', 'b'))),
        call(str(Path('dir', 'subdir', 'c'))),
        call(str(Path('dir', 'subdir', 'nested', 'd'))),
    ],
                          any_order=True)


def test_download_folder(tmp_directory, monkeypatch, mock_client):
    Path('backup', 'dir', 'subdir', 'nested').mkdir(parents=True)
    Path('backup', 'dir', 'a').touch()
    Path('backup', 'dir', 'b').touch()
    Path('backup', 'dir', 'subdir', 'c').touch()
    Path('backup', 'dir', 'subdir', 'nested', 'd').touch()

    mock_blobs = []

    for name in [
            'backup/dir/a', 'backup/dir/b', 'backup/dir/subdir/c',
            'backup/dir/subdir/nested/d'
    ]:
        m = Mock()
        m.name = name
        mock_blobs.append(m)

    mock = Mock()
    client = GCloudStorageClient('my-bucket-name',
                                 parent='backup',
                                 path_to_project_root='.')
    monkeypatch.setattr(client, '_download', mock)
    # simulate this is not a file
    mock_client().bucket().blob('backup/a').exists.return_value = False
    mock_client().bucket().client.list_blobs.return_value = mock_blobs

    client.download('dir')

    mock_blobs[0].download_to_filename.assert_called_once_with(Path('dir/a'))
    mock_blobs[1].download_to_filename.assert_called_once_with(Path('dir/b'))
    mock_blobs[2].download_to_filename.assert_called_once_with(
        Path('dir/subdir/c'))
    mock_blobs[3].download_to_filename.assert_called_once_with(
        Path('dir/subdir/nested/d'))


def test_download_with_custom_destination(monkeypatch, mock_client):
    mock = Mock()
    client = GCloudStorageClient('my-bucket-name',
                                 parent='parent',
                                 path_to_project_root='.')
    monkeypatch.setattr(client, '_download', mock)

    client.download('file.txt', destination='another.txt')

    mock.assert_called_once_with('another.txt',
                                 str(PurePosixPath('parent', 'file.txt')))


def test_pickle(mock_client):
    c = GCloudStorageClient('my-bucket-name',
                            parent='',
                            path_to_project_root='.')
    pickle.loads(pickle.dumps(c))


def test_pickle_from_service_acccount(mock_client):
    c = GCloudStorageClient('my-bucket-name',
                            'folder',
                            'my.json',
                            path_to_project_root='.')
    pickle.loads(pickle.dumps(c))


def test_close(mock_client):
    GCloudStorageClient('my-bucket-name', parent='',
                        path_to_project_root='.').close()


@pytest.mark.parametrize('arg, expected', [
    ['file.txt', ('backup', 'file.txt')],
    ['subdir/file.txt', ('backup', 'subdir', 'file.txt')],
])
def test_remote_path(tmp_directory, arg, expected, mock_client):
    client = GCloudStorageClient('my-bucket-name',
                                 parent='backup',
                                 path_to_project_root='.')

    assert PurePosixPath(client._remote_path(arg)) == PurePosixPath(*expected)


def test_remote_exists(monkeypatch, mock_client):
    client = GCloudStorageClient('my-bucket-name',
                                 parent='backup',
                                 path_to_project_root='.')

    client._remote_exists('a')

    mock_client().bucket().blob('backup/a').exists.assert_called_once_with()


def test_is_file(monkeypatch, mock_client):
    client = GCloudStorageClient('my-bucket-name',
                                 parent='backup',
                                 path_to_project_root='.')
    # simulate file exists
    client._bucket.blob('backup/a').exists.return_value = True

    assert client._is_file('backup/a')


def test_remote_exists_directory(monkeypatch, mock_client):
    client = GCloudStorageClient('my-bucket-name',
                                 parent='backup',
                                 path_to_project_root='.')
    bucket = mock_client().bucket()
    # simulate file does not exist
    bucket.blob('backup/a').exists.return_value = False
    # but directory does
    bucket.client.list_blobs.return_value = ['file', 'another']

    client._remote_exists('a')

    bucket.blob('backup/a').exists.assert_called_once_with()
    bucket.client.list_blobs.assert_called_once_with('my-bucket-name',
                                                     prefix='backup/a')


def test_is_dir(monkeypatch, mock_client):
    client = GCloudStorageClient('my-bucket-name',
                                 parent='backup',
                                 path_to_project_root='.')
    # test dir exists
    client._bucket.client.list_blobs.return_value = ['file', 'another']

    assert client._is_dir('backup/a')


def test_can_initialize_if_valid_project_root(tmp_directory, mock_client):
    Path('pipeline.yaml').touch()
    client = GCloudStorageClient('some-bucket', 'some-folder')
    assert client._path_to_project_root == Path(tmp_directory).resolve()


def test_error_if_missing_project_root(tmp_directory):

    with pytest.raises(DAGSpecInvalidError) as excinfo:
        GCloudStorageClient('some-bucket', 'some-folder')

    assert 'Cannot initialize' in str(excinfo.value)


def test_parent(tmp_directory, mock_client):
    Path('pipeline.yaml').touch()
    c = GCloudStorageClient('some-bucket', 'some-folder')
    assert c.parent == 'some-folder'
