from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.executors import Serial
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.clients import LocalStorageClient


def _touch(product):
    Path(product).touch()


def _touch_upstream(product, upstream):
    Path(product).touch()


def test_upload_file(tmp_directory_with_project_root):
    Path('backup').mkdir()
    Path('file').write_text('content')
    client = LocalStorageClient('backup')

    client.upload('file')

    assert Path('backup', 'file').read_text() == 'content'


def test_upload_directory(tmp_directory_with_project_root):
    Path('backup').mkdir()
    Path('dir').mkdir()
    Path('dir', 'file').write_text('content')
    client = LocalStorageClient('backup')

    client.upload('dir')

    assert Path('backup', 'dir', 'file').read_text() == 'content'


def test_download_file(tmp_directory_with_project_root):
    Path('backup').mkdir()
    Path('backup', 'file').write_text('content')
    client = LocalStorageClient('backup')

    client.download('file')

    assert Path('file').read_text() == 'content'


def test_download_file_nested_dir(tmp_directory_with_project_root):
    Path('backup', 'nested').mkdir(parents=True)
    Path('backup', 'nested', 'file').write_text('content')
    client = LocalStorageClient('backup')

    client.download('nested/file')

    assert Path('nested', 'file').read_text() == 'content'


def test_download_file_custom_destination(tmp_directory_with_project_root):
    Path('backup').mkdir()
    Path('backup', 'file').write_text('content')
    client = LocalStorageClient('backup')

    client.download('file', destination='another')

    assert Path('another').read_text() == 'content'


def test_download_directory(tmp_directory_with_project_root):
    Path('backup', 'dir').mkdir(parents=True)
    Path('backup', 'dir', 'file').write_text('content')
    client = LocalStorageClient('backup')

    client.download('dir')

    assert Path('dir', 'file').read_text() == 'content'


def test_remote_exists(tmp_directory_with_project_root):
    Path('backup').mkdir()
    Path('backup', 'file').write_text('content')
    client = LocalStorageClient('backup')

    assert client._remote_exists('file')


def test_creates_directory(tmp_directory_with_project_root):
    LocalStorageClient(str(Path('my', 'backup', 'dir')))
    assert Path('my', 'backup', 'dir').is_dir()


@pytest.mark.parametrize('arg, expected', [
    ['file.txt', ('backup', 'file.txt')],
    ['subdir/file.txt', ('backup', 'subdir', 'file.txt')],
])
def test_remote_path(tmp_directory_with_project_root, arg, expected):
    client = LocalStorageClient('backup')
    assert client._remote_path(arg) == Path(*expected)


def test_error_if_not_in_project_path(tmp_directory):
    with pytest.raises(ValueError) as excinfo:
        LocalStorageClient('backup')

    expected = "Could not determine project's root directory"
    assert expected in str(excinfo.value)


def test_keeps_folder_layout(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.clients[File] = LocalStorageClient('backup', path_to_project_root='.')
    Path('dir').mkdir()
    PythonCallable(_touch, File('file'), dag, name='task')
    PythonCallable(_touch, File('dir/nested'), dag, name='nested')
    dag.build()

    assert Path('backup', 'dir', 'nested').is_file()
    assert Path('backup', 'dir', '.nested.metadata').is_file()
    assert Path('backup', 'file').is_file()
    assert Path('backup', '.file.metadata').is_file()
