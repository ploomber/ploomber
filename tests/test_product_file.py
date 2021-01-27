import sys
from pathlib import Path
from unittest.mock import Mock, _Call

import pytest

from ploomber.executors import Serial, Parallel
from ploomber.products import File
from ploomber.tasks import PythonCallable
from ploomber.constants import TaskStatus
from ploomber.exceptions import DAGBuildError, DAGRenderError
from ploomber import DAG

# TODO: test download/upload folder


def _touch(product):
    Path(str(product)).touch()


def test_file_initialized_with_str():
    f = File('/path/to/file')
    f.render({})
    assert str(f) == '/path/to/file'


def test_file_initialized_with_path():
    path = Path('/path/to/file')
    f = File(path)
    f.render({})
    assert str(f) == str(path)


def test_file_is_rendered_correctly():
    f = File('/path/to/{{name}}')
    f.render(params=dict(name='file'))
    assert str(f) == '/path/to/file'


def test_file_delete(tmp_directory):
    f = Path('file')
    f.touch()
    File('file').delete()

    assert not f.exists()


def test_file_delete_directory(tmp_directory):
    d = Path('dir')
    d.mkdir()
    (d / 'file.txt').touch()

    File('dir').delete()

    assert not d.exists()


def test_delete_non_existing_metadata(tmp_directory):
    File('some_file')._delete_metadata()
    assert not Path('some_file.source').exists()


def test_delete_metadata(tmp_directory):
    Path('some_file.source').touch()
    File('some_file')._delete_metadata()
    assert not Path('some_file.source').exists()


@pytest.mark.skipif(sys.platform == "win32",
                    reason="Windows has a different path representation")
def test_repr_relative():
    assert repr(File('a/b/c')) == "File('a/b/c')"


@pytest.mark.skipif(sys.platform == "win32",
                    reason="Windows has a different path representation")
def test_repr_absolute():
    assert repr(File('/a/b/c')) == "File('/a/b/c')"


def test_repr_absolute_shows_as_relative_if_possible():
    path = Path('.').resolve() / 'a'
    assert repr(File(path)) == "File('a')"


def test_client_is_none_by_default():
    dag = DAG()
    product = File('file.txt')
    PythonCallable(_touch, product, dag=dag)
    assert product.client is None


def test_task_level_client():
    dag = DAG()
    dag.clients[File] = Mock()
    client = Mock()
    product = File('file.txt', client=client)
    PythonCallable(_touch, product, dag=dag)
    assert product.client is client


def test_dag_level_client():
    dag = DAG()
    client = Mock()
    dag.clients[File] = client
    product = File('file.txt')
    PythonCallable(_touch, product, dag=dag)
    assert product.client is client


def test_download(tmp_directory):
    client = Mock()
    product = File('file.txt', client=client)

    product.download()

    assert client.download.call_args_list == [(('file.txt.source', ), ),
                                              (('file.txt', ), )]


def test_upload(tmp_directory):
    Path('file.txt').touch()
    Path('file.txt.source').touch()
    client = Mock()
    product = File('file.txt', client=client)

    product.upload()

    assert client.upload.call_args_list == [(('file.txt.source', ), ),
                                            (('file.txt', ), )]


@pytest.mark.parametrize('to_touch', [
    ['file.txt'],
    ['file.txt.source'],
    [],
])
def test_do_not_upload_if_none_or_one(to_touch, tmp_directory):
    for f in to_touch:
        Path(f).touch()

    client = Mock()
    product = File('file.txt', client=client)

    product.upload()

    client.upload.assert_not_called()


@pytest.mark.parametrize('to_touch', [
    ['file.txt'],
    ['file.txt.source'],
    ['file.txt', 'file.txt.source'],
])
def test_do_not_download_if_file_or_metadata_exists(to_touch, tmp_directory):
    for f in to_touch:
        Path(f).touch()

    client = Mock()
    product = File('file.txt', client=client)

    product.download()

    client.download.assert_not_called()


@pytest.mark.parametrize('remote_exists', [
    [False, True],
    [False, False],
])
def test_do_not_download_if_metadata_does_not_exist_in_remote(
        remote_exists, tmp_directory):
    client = Mock()
    client._remote_exists.side_effect = remote_exists
    product = File('file.txt', client=client)

    product.download()

    # should call this to verify if the remote copy exists
    client._remote_exists.assert_called_with('file.txt.source')
    client._remote_exists.assert_called_once()
    # should not attempt to download
    client.download.assert_not_called()


def test_do_not_download_if_file_does_not_exist_in_remote(tmp_directory):
    client = Mock()
    client._remote_exists.side_effect = [True, False]
    product = File('file.txt', client=client)

    product.download()

    # should call twice, since metadata exists, it has to also check the file
    client._remote_exists.assert_has_calls([
        _Call((('file.txt.source', ), {})),
        _Call((('file.txt', ), {})),
    ])
    # should not attempt to download
    client.download.assert_not_called()


def test_download_upload_without_client():
    dag = DAG()
    product = File('file.txt')
    PythonCallable(_touch, product, dag=dag)

    # this shouldn't crash
    product.download()
    product.upload()


def test_do_not_upload_after_task_build(tmp_directory):
    """
    Currently, uploading logic happens in the executor, that's why task.build()
    doesn't trigger it
    """
    dag = DAG()
    product = File('file.txt')
    product.upload = Mock(wraps=product.upload)
    task = PythonCallable(_touch, product, dag=dag)
    task.build()

    product.upload.assert_not_called()


# NOTE: the following tests check File behavior when a DAG is executed,
# depending on dag.executor configuration, tasks might run in a child process
# which causes the monkeypatch + Mock combination not to work correctly, to
# support testing, we implement a subclasses instead


class FileWithUploadCounter(File):
    def upload(self):
        path = Path('upload_count')

        if not path.exists():
            path.write_text('1')
        else:
            path.write_text(str(int(path.read_text()) + 1))

    def _get_call_count(self):
        return int(Path('upload_count').read_text())


class FileWithUploadError(File):
    def upload(self):
        raise ValueError('upload failed!')


class FileWithDownloadError(File):
    def download(self):
        raise ValueError('download failed!')


@pytest.mark.parametrize('executor', [
    Serial(build_in_subprocess=False),
    Serial(build_in_subprocess=True),
    Parallel(),
])
def test_upload_after_dag_build(tmp_directory, monkeypatch, executor):
    dag = DAG(executor=executor)
    product = FileWithUploadCounter('file.txt')
    PythonCallable(_touch, product, dag=dag)

    # when building for the first time, upload should be called
    dag.build()
    assert product._get_call_count() == 1

    # the second time, tasks are skipped so product still has call count at 1
    dag.build()
    assert product._get_call_count() == 1


@pytest.mark.parametrize('executor', [
    Serial(build_in_subprocess=False),
    Serial(build_in_subprocess=True),
    Parallel(),
])
def test_upload_error(executor, tmp_directory, monkeypatch):
    dag = DAG(executor=executor)

    product = FileWithUploadError('file.txt')
    PythonCallable(_touch, product, dag=dag)

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    assert 'upload failed!' in str(excinfo.getrepr())
    assert dag['_touch'].exec_status == TaskStatus.Errored


@pytest.mark.parametrize('executor', [
    Serial(build_in_subprocess=False),
    Serial(build_in_subprocess=True),
    Parallel(),
])
def test_download_error(executor, monkeypatch, tmp_directory):
    dag = DAG(executor=executor)

    product = FileWithDownloadError('file.txt')
    PythonCallable(_touch, product, dag=dag)

    with pytest.raises(DAGRenderError) as excinfo:
        dag.build()

    assert 'download failed!' in str(excinfo.getrepr())
    assert dag['_touch'].exec_status == TaskStatus.ErroredRender


def test_attempts_to_download_on_each_build(tmp_directory, monkeypatch):
    # run in the same process, otherwise we won't know if the mock object
    # is called
    dag = DAG(executor=Serial(build_in_subprocess=False))
    product = File('file.txt')
    PythonCallable(_touch, product, dag=dag)

    monkeypatch.setattr(File, 'download', Mock(wraps=product.download))

    # download is called on each call to dag.render(), dag.build() calls it...
    dag.build()
    assert product.download.call_count == 1

    # second time, it should attempt to download again as the remote files
    # could've been modified
    dag.build()
    assert product.download.call_count == 2
