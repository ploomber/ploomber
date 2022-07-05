import pickle
import shutil
from itertools import chain
from glob import iglob
from pathlib import Path, PurePosixPath

import pytest
import boto3
from moto import mock_s3

from ploomber.clients.storage.aws import S3Client
from ploomber.exceptions import RemoteFileNotFound, DAGSpecInvalidError


@pytest.fixture(scope='function')
def s3():
    with mock_s3():
        client = boto3.client('s3', region_name='us-east-1')
        client.create_bucket(Bucket='some-bucket')
        yield client


@pytest.mark.parametrize('arg', ['my-file', Path('my-file')])
def test_underscore_upload(s3, tmp_directory, arg):
    Path('my-file').touch()

    client = S3Client('some-bucket', 'my-folder', path_to_project_root='.')
    client._upload(arg)

    contents = s3.list_objects(Bucket='some-bucket')['Contents']

    assert len(contents) == 1
    assert contents[0]['Key'] == 'my-folder/my-file'


@pytest.mark.parametrize('arg', ['my-file', Path('my-file')])
def test_underscore_download(s3, tmp_directory, arg):
    Path('some-file-to-upload').touch()
    s3.upload_file('some-file-to-upload', 'some-bucket', 'my-folder/my-file')

    client = S3Client('some-bucket', 'my-folder', path_to_project_root='.')
    client._download(arg, 'my-folder/my-file')

    assert Path('my-file').exists()


@pytest.mark.parametrize('parent', ['', 'some/parent'])
@pytest.mark.parametrize('file_', ['file.txt', 'subdir/file.txt'])
def test_upload_file(s3, parent, tmp_directory, file_):
    Path(file_).parent.mkdir(exist_ok=True)
    Path(file_).touch()
    client = S3Client('some-bucket', parent=parent, path_to_project_root='.')

    client.upload(file_)

    contents = s3.list_objects(Bucket='some-bucket')['Contents']

    assert len(contents) == 1
    assert contents[0]['Key'] == str(PurePosixPath(parent, file_))


def test_upload_folder(s3, tmp_directory):
    Path('dir', 'subdir', 'nested').mkdir(parents=True)
    Path('dir', 'a').touch()
    Path('dir', 'b').touch()
    Path('dir', 'subdir', 'c').touch()
    Path('dir', 'subdir', 'nested', 'd').touch()
    client = S3Client('some-bucket',
                      parent='my-folder',
                      path_to_project_root='.')

    client.upload('dir')

    contents = s3.list_objects(Bucket='some-bucket')['Contents']

    assert set(c['Key'] for c in contents) == {
        'my-folder/dir/a', 'my-folder/dir/b', 'my-folder/dir/subdir/c',
        'my-folder/dir/subdir/nested/d'
    }


@pytest.mark.parametrize('destination, expected', [
    [None, 'dir/a'],
    ['dir/b', 'dir/b'],
],
                         ids=['destination-default', 'destination-custom'])
def test_download(s3, tmp_directory, destination, expected):
    path = Path('dir', 'a')
    path.parent.mkdir()

    path.write_text('hello')
    s3.upload_file('dir/a', 'some-bucket',
                   str(PurePosixPath('my-folder', 'dir/a')))
    path.unlink()

    client = S3Client('some-bucket',
                      parent='my-folder',
                      path_to_project_root='.')

    client.download('dir/a', destination=destination)

    assert Path(expected).read_text() == 'hello'


def test_error_when_downloading_non_existing(s3):
    client = S3Client('some-bucket',
                      parent='my-folder',
                      path_to_project_root='.')

    with pytest.raises(RemoteFileNotFound) as excinfo:
        client.download('some-file')

    assert "Could not download 'some-file' using client" in str(excinfo.value)


def test_download_folder(s3, tmp_directory):
    Path('dir', 'subdir', 'nested').mkdir(parents=True)
    Path('dir', 'a').touch()
    Path('dir', 'b').touch()
    Path('dir', 'subdir', 'c').touch()
    Path('dir', 'subdir', 'nested', 'd').touch()

    for path in chain(iglob('dir/*'), iglob('dir/*/*'), iglob('dir/*/*/*')):
        path = Path(path)

        if path.is_file():
            s3.upload_file(str(path), 'some-bucket',
                           str(PurePosixPath('my-folder', *path.parts)))

    shutil.rmtree('dir')

    client = S3Client('some-bucket',
                      parent='my-folder',
                      path_to_project_root='.')

    client.download('dir')

    assert Path('dir', 'a').exists()
    assert Path('dir', 'b').exists()
    assert Path('dir', 'subdir', 'c').exists()
    assert Path('dir', 'subdir', 'nested', 'd').exists()


def test_is_file(s3, tmp_directory):
    path = Path('dir', 'a')
    path.parent.mkdir()

    path.write_text('hello')
    s3.upload_file('dir/a', 'some-bucket',
                   str(PurePosixPath('my-folder', 'dir/a')))
    path.unlink()

    client = S3Client('some-bucket',
                      parent='my-folder',
                      path_to_project_root='.')

    assert client._is_file('my-folder/dir/a')
    assert not client._is_file('my-folder/dir/b')


def test_is_dir(s3, tmp_directory):
    path = Path('dir', 'a')
    path.parent.mkdir()

    path.write_text('hello')
    s3.upload_file('dir/a', 'some-bucket',
                   str(PurePosixPath('my-folder', 'dir/a')))

    client = S3Client('some-bucket',
                      parent='my-folder',
                      path_to_project_root='.')

    assert client._is_dir('my-folder/dir/')


def test_pickle():
    c = S3Client('some-bucket', parent='my-folder', path_to_project_root='.')
    pickle.loads(pickle.dumps(c))


def test_close():
    S3Client('some-bucket', parent='my-folder',
             path_to_project_root='.').close()


def test_can_initialize_if_valid_project_root(tmp_directory):
    Path('pipeline.yaml').touch()
    client = S3Client('some-bucket', 'some-folder')
    assert client._path_to_project_root == Path(tmp_directory).resolve()


def test_error_if_missing_project_root(tmp_directory):

    with pytest.raises(DAGSpecInvalidError) as excinfo:
        S3Client('some-bucket', 'some-folder')

    assert 'Cannot initialize' in str(excinfo.value)


def test_parent(tmp_directory):
    Path('pipeline.yaml').touch()
    c = S3Client('some-bucket', 'some-folder')
    assert c.parent == 'some-folder'
