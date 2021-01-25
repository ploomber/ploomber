import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from ploomber.products import File
from ploomber.tasks import PythonCallable
from ploomber import DAG


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
def test_do_not_download_if_any(to_touch, tmp_directory):
    for f in to_touch:
        Path(f).touch()

    client = Mock()
    product = File('file.txt', client=client)

    product.download()

    client.download.assert_not_called()


def test_download_upload_without_client():
    dag = DAG()
    product = File('file.txt')
    PythonCallable(_touch, product, dag=dag)

    # this shouldn't crash
    product.download()
    product.upload()


def test_upload_after_task_build(tmp_directory):
    dag = DAG()
    product = File('file.txt')
    product.upload = Mock(wraps=product.upload)
    task = PythonCallable(_touch, product, dag=dag)
    task.build()

    product.upload.assert_called_once()


# TODO: test with dag.build, make sure upload is called with all executors as they are responsible for saving metadata, and right after, to call upload

# TODO: test download when remote copy does not exist (should not attempt to download)

# TODO: when upload fails, task should fail as well
# TODO: when download fails, task should fail as well
# TODO: test ith dag.build. should not call upload when task is skipped
# TODO: test MetaProduct implementation upload,download