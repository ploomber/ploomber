from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.executors import Serial
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.clients import LocalStorageClient
from ploomber.constants import TaskStatus
from ploomber.exceptions import RemoteFileNotFound, DAGSpecInvalidError


def _touch(product):
    Path(product).touch()


def _touch_many(product):
    for prod in product:
        Path(prod).touch()


def _touch_upstream(product, upstream):
    Path(product).touch()


def test_upload_file(tmp_directory):
    Path('backup').mkdir()
    Path('file').write_text('content')
    client = LocalStorageClient('backup', path_to_project_root='.')

    client.upload('file')

    assert Path('backup', 'file').read_text() == 'content'


def test_upload_directory(tmp_directory):
    Path('backup').mkdir()
    Path('dir').mkdir()
    Path('dir', 'file').write_text('content')
    client = LocalStorageClient('backup', path_to_project_root='.')

    client.upload('dir')

    assert Path('backup', 'dir', 'file').read_text() == 'content'


def test_download_file(tmp_directory):
    Path('backup').mkdir()
    Path('backup', 'file').write_text('content')
    client = LocalStorageClient('backup', path_to_project_root='.')

    client.download('file')

    assert Path('file').read_text() == 'content'


def test_error_when_downloading_non_existing(tmp_directory):
    client = LocalStorageClient('backup', path_to_project_root='.')

    with pytest.raises(RemoteFileNotFound) as excinfo:
        client.download('file')

    expected = ("Could not download 'file' using client "
                "LocalStorageClient('backup'): No such file or directory")
    assert expected == str(excinfo.value)


def test_download_file_nested_dir(tmp_directory):
    Path('backup', 'nested').mkdir(parents=True)
    Path('backup', 'nested', 'file').write_text('content')
    client = LocalStorageClient('backup', path_to_project_root='.')

    client.download('nested/file')

    assert Path('nested', 'file').read_text() == 'content'


def test_download_file_custom_destination(tmp_directory):
    Path('backup').mkdir()
    Path('backup', 'file').write_text('content')
    client = LocalStorageClient('backup', path_to_project_root='.')

    client.download('file', destination='another')

    assert Path('another').read_text() == 'content'


def test_download_directory(tmp_directory):
    Path('backup', 'dir').mkdir(parents=True)
    Path('backup', 'dir', 'file').write_text('content')
    client = LocalStorageClient('backup', path_to_project_root='.')

    client.download('dir')

    assert Path('dir', 'file').read_text() == 'content'


def test_remote_exists(tmp_directory):
    Path('backup').mkdir()
    Path('backup', 'file').write_text('content')
    client = LocalStorageClient('backup', path_to_project_root='.')

    assert client._remote_exists('file')


def test_creates_directory(tmp_directory):
    LocalStorageClient(str(Path('my', 'backup', 'dir')),
                       path_to_project_root='.')
    assert Path('my', 'backup', 'dir').is_dir()


@pytest.mark.parametrize('arg, expected', [
    ['file.txt', ('backup', 'file.txt')],
    ['subdir/file.txt', ('backup', 'subdir', 'file.txt')],
])
def test_remote_path(tmp_directory, arg, expected):
    client = LocalStorageClient('backup', path_to_project_root='.')
    assert client._remote_path(arg) == Path(*expected)


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


def test_outdated_task_with_metaproduct_downloads_metadata(tmp_directory):
    def _make():
        dag = DAG(executor=Serial(build_in_subprocess=True))
        dag.clients[File] = LocalStorageClient('backup',
                                               path_to_project_root='.')
        t1 = PythonCallable(_touch_many, {
            'one': File('one'),
            'two': File('two')
        },
                            dag,
                            name='task')
        t2 = PythonCallable(_touch_upstream,
                            File('three'),
                            dag,
                            name='another')
        t1 >> t2
        return dag

    _make().build()

    Path('one').unlink()

    # this triggers remote metadata download to determine status
    dag = _make().render()

    status = {n: t.exec_status for n, t in dag.items()}

    assert status['task'] == TaskStatus.WaitingDownload
    assert status['another'] == TaskStatus.Skipped


def test_can_initialize_if_valid_project_root(tmp_directory):
    Path('pipeline.yaml').touch()
    client = LocalStorageClient('remote')
    assert client._path_to_project_root == Path(tmp_directory).resolve()


def test_error_if_missing_project_root(tmp_directory):

    with pytest.raises(DAGSpecInvalidError) as excinfo:
        LocalStorageClient('remote')

    assert 'Cannot initialize' in str(excinfo.value)


def test_parent(tmp_directory):
    Path('pipeline.yaml').touch()
    c = LocalStorageClient('remote')
    assert c.parent == 'remote'
