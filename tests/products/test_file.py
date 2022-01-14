import json
import os
import sys
from pathlib import Path
from unittest.mock import Mock, call
import string
from collections.abc import Mapping

import pytest

from ploomber.executors import Serial, Parallel
from ploomber.products import File
from ploomber.tasks import PythonCallable
from ploomber.constants import TaskStatus
from ploomber.exceptions import DAGBuildError, TaskBuildError
from ploomber.clients.storage.local import LocalStorageClient
from ploomber import DAG


def _touch(product):
    Path(str(product)).touch()


def _touch_upstream(product, upstream):
    Path(upstream['root']).read_text()

    # if metaproduct...
    if isinstance(product, Mapping):
        for prod in product:
            Path(str(prod)).touch()
    # if single product...
    else:
        Path(str(product)).touch()


def _load_json(path):
    return json.loads(Path(path).read_text())


def _write_json(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f)


def _make_dag(with_client=True):
    dag = DAG(executor=Serial(build_in_subprocess=False))

    if with_client:
        dag.clients[File] = LocalStorageClient('remote',
                                               path_to_project_root='.')

    root = PythonCallable(_touch, File('root'), dag=dag, name='root')
    task = PythonCallable(_touch_upstream,
                          File('file.txt'),
                          dag=dag,
                          name='task')
    root >> task
    return dag


def _make_dag_with_two_upstream():
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.clients[File] = LocalStorageClient('remote', path_to_project_root='.')

    root = PythonCallable(_touch, File('root'), dag=dag, name='root')
    another = PythonCallable(_touch, File('another'), dag=dag, name='another')
    task = PythonCallable(_touch_upstream,
                          File('file.txt'),
                          dag=dag,
                          name='task')
    (root + another) >> task
    return dag


def _make_dag_with_metaproduct(with_client=True):
    dag = DAG(executor=Serial(build_in_subprocess=False))

    if with_client:
        dag.clients[File] = LocalStorageClient('remote',
                                               path_to_project_root='.')

    root = PythonCallable(_touch, File('root'), dag=dag, name='root')
    task = PythonCallable(_touch_upstream, {
        'one': File('file.txt'),
        'another': File('another.txt')
    },
                          dag=dag,
                          name='task')
    root >> task
    return dag


@pytest.fixture
def dag():
    return _make_dag(with_client=True)


def test_is_a_file_like_object():
    assert isinstance(File('/path/to/file'), os.PathLike)


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


def test_path_to_metadata():
    assert File('file.txt')._path_to_metadata == Path('.file.txt.metadata')


def test_migrates_metadata(tmp_directory):
    Path('sample.txt.source').write_text(
        '{"timestamp": "t", "stored_source_code": "c"}')
    Path('sample.txt').touch()
    meta = File('sample.txt').fetch_metadata()

    assert not Path('sample.txt.source').exists()
    assert Path('.sample.txt.metadata').exists()
    assert meta['stored_source_code'] == 'c'
    assert meta['timestamp'] == 't'


def test_delete_non_existing_metadata(tmp_directory):
    File('some_file')._delete_metadata()
    assert not Path('.some_file.metadata').exists()


@pytest.mark.parametrize('file_, metadata', [
    ['some_file', '.some_file.metadata'],
    ['dir/some_file', 'dir/.some_file.metadata'],
])
def test_delete_metadata(tmp_directory, file_, metadata):
    Path('dir').mkdir()
    Path(metadata).touch()
    File(file_)._delete_metadata()
    assert not Path(metadata).exists()


def test_error_on_corrupted_metadata(tmp_directory):
    Path('corrupted').touch()
    Path('.corrupted.metadata').write_text('this is not json')

    with pytest.raises(ValueError) as excinfo:
        File('corrupted').fetch_metadata()

    msg = ("Error loading JSON metadata for File('corrupted') "
           "stored at '.corrupted.metadata'")
    assert msg == str(excinfo.value)


def test_error_when_initializing_with_obj_other_than_str_or_path():
    with pytest.raises(TypeError) as excinfo:
        File(dict())

    msg = 'File must be initialized with a str or a pathlib.Path'
    assert str(excinfo.value) == msg


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


def test_repr_short():
    expected = "File('abcdefghijklmnopq...IJKLMNOPQRSTUVWXYZ')"
    assert repr(File(string.ascii_letters)) == expected


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


def test_build_triggers_metadata_download(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))

    def download(path, destination=None):
        Path(path).touch()

    client = Mock()
    client.download.side_effect = download

    product = File('file.txt', client=client)
    PythonCallable(_touch, product, dag=dag)

    # this should download files instead of executing the task
    dag.build()

    client.download.assert_called_with(
        Path('.file.txt.metadata'),
        destination=Path('.file.txt.metadata.remote'))


def test_product_upload_uploads_metadata_and_product(tmp_directory):
    Path('file.txt').touch()
    Path('.file.txt.metadata').touch()
    client = Mock()
    product = File('file.txt', client=client)

    product.upload()

    client.upload.assert_has_calls(
        [call(Path('.file.txt.metadata')),
         call(Path('file.txt'))])


def test_task_without_client_is_outdated_by_data(tmp_directory):
    dag = _make_dag(with_client=False)
    t = dag['task']
    dag.build()
    # clear metadata to force reload
    t.product.metadata.clear()

    # change timestamp to make this outdated by data (upstream timestamp
    # will be higher)
    m = _load_json('.file.txt.metadata')
    m['timestamp'] = 0
    _write_json(m, '.file.txt.metadata')

    assert t.product._is_outdated()
    assert t.product._outdated_data_dependencies()


def test_task_without_client_is_outdated_by_code(tmp_directory):
    dag = _make_dag(with_client=False)
    t = dag['task']
    dag.build()
    # clear metadata to force reload
    t.product.metadata.clear()

    # simulate outdated task
    m = _load_json('.file.txt.metadata')
    m['stored_source_code'] = m['stored_source_code'] + '1 + 1\n'
    _write_json(m, '.file.txt.metadata')

    assert t.product._outdated_code_dependency()
    assert t.product._is_outdated()


def _edit_source_code(path):
    m = _load_json(path)
    m['stored_source_code'] = m['stored_source_code'] + '1 + 1\n'
    _write_json(m, path)


def _delete_metadata(path):
    Path(path).unlink()


@pytest.mark.parametrize('operation', [_edit_source_code, _delete_metadata])
def test_task_with_client_is_not_outdated_returns_waiting_download(
        operation, tmp_directory):
    dag = _make_dag(with_client=True)
    dag.build()

    # simulate local outdated tasks
    operation('.file.txt.metadata')
    operation('.root.metadata')

    dag = _make_dag(with_client=True).render()

    assert dag['root'].product._is_outdated() == TaskStatus.WaitingDownload
    assert dag['task'].product._is_outdated() == TaskStatus.WaitingDownload
    assert set(v.exec_status
               for v in dag.values()) == {TaskStatus.WaitingDownload}


@pytest.mark.parametrize('operation', [_edit_source_code, _delete_metadata])
def test_task_with_client_and_metaproduct_isnt_outdated_rtrns_waiting_download(
        operation, tmp_directory):
    """
    Checking MetaProduct correctly forwards WaitingDownload when calling
    MetaProduct._is_outdated
    """
    dag = _make_dag_with_metaproduct(with_client=True)
    dag.build()

    # simulate local outdated tasks
    operation('.file.txt.metadata')
    operation('.another.txt.metadata')
    operation('.root.metadata')

    dag = _make_dag_with_metaproduct(with_client=True).render()

    assert dag['root'].product._is_outdated() == TaskStatus.WaitingDownload
    assert dag['task'].product._is_outdated() == TaskStatus.WaitingDownload
    assert set(v.exec_status
               for v in dag.values()) == {TaskStatus.WaitingDownload}


@pytest.mark.parametrize('operation', [_edit_source_code, _delete_metadata])
def test_task_with_client_and_metaproduct_with_some_missing_products(
        operation, tmp_directory):
    """
    If local MetaProduct content isn't consistent, it should execute instead of
    download
    """
    dag = _make_dag_with_metaproduct(with_client=True)
    dag.build()

    # simulate *some* local outdated tasks
    operation('.file.txt.metadata')
    operation('.root.metadata')

    dag = _make_dag_with_metaproduct(with_client=True).render()

    assert dag['root'].product._is_outdated() == TaskStatus.WaitingDownload
    assert dag['task'].product._is_outdated() == TaskStatus.WaitingDownload
    assert dag['root'].exec_status == TaskStatus.WaitingDownload
    assert dag['task'].exec_status == TaskStatus.WaitingDownload


def test_downloads_if_missing_some_products_in_metaproduct(tmp_directory):
    dag = _make_dag_with_metaproduct(with_client=True)
    dag.build()

    # simulate *some* local outdated tasks
    _delete_metadata('.file.txt.metadata')

    dag = _make_dag_with_metaproduct(with_client=True).render()

    assert not dag['root'].product._is_outdated()
    assert dag['root'].exec_status == TaskStatus.Skipped
    assert dag['task'].product._is_outdated() == TaskStatus.WaitingDownload
    assert dag['task'].exec_status == TaskStatus.WaitingDownload


@pytest.mark.parametrize('operation', [_edit_source_code, _delete_metadata])
def test_task_with_client_and_metaproduct_with_some_missing_remote_products(
        operation, tmp_directory):
    """
    If remote MetaProduct content isn't consistent, it should execute instead
    of download
    """
    dag = _make_dag_with_metaproduct(with_client=True)
    dag.build()

    # simulate *some* local outdated tasks (to force remote metadata lookup)
    operation('.file.txt.metadata')
    operation('.root.metadata')

    # simulate corrupted remote MetaProduct metadata
    operation('remote/.file.txt.metadata')

    dag = _make_dag_with_metaproduct(with_client=True).render()

    assert dag['root'].product._is_outdated() == TaskStatus.WaitingDownload
    assert dag['task'].product._is_outdated() is True
    assert dag['root'].exec_status == TaskStatus.WaitingDownload
    assert dag['task'].exec_status == TaskStatus.WaitingUpstream


def test_task_with_skipped_and_waiting_to_download_upstream_downloads(
        tmp_directory):
    _make_dag_with_two_upstream().build()

    # force an upstream task to be waiting for download
    # (the other should be skipped since we didn't change anything)
    Path('root').unlink()

    dag = _make_dag_with_two_upstream().render()

    assert dag['task'].exec_status == TaskStatus.Skipped


def test_task_with_client_does_not_return_waiting_download_if_outdated_remote(
        tmp_directory):
    dag = _make_dag(with_client=True)
    dag.build()

    # simulate outdated remote metadata (no need to download)
    m = _load_json('remote/.file.txt.metadata')
    m['stored_source_code'] = m['stored_source_code'] + '1 + 1\n'
    _write_json(m, 'remote/.file.txt.metadata')

    dag = _make_dag(with_client=True)

    assert not dag['root'].product._is_outdated()


def test_task_with_client_is_not_outdated_after_build(tmp_directory):
    dag = _make_dag(with_client=True)
    dag.build()

    dag = _make_dag(with_client=True)

    assert not dag['root'].product._is_outdated()
    assert not dag['task'].product._is_outdated()


@pytest.mark.parametrize('to_touch', [
    ['file.txt'],
    ['.file.txt.metadata'],
    [],
])
def test_upload_error_if_metadata_or_product_do_not_exist(
        to_touch, tmp_directory):
    for f in to_touch:
        Path(f).touch()

    client = Mock()
    product = File('file.txt', client=client)

    with pytest.raises(RuntimeError):
        product.upload()

    client.upload.assert_not_called()


def test_download_triggers_client_download(tmp_directory):
    client = Mock()
    product = File('file.txt', client=client)

    product.download()

    client.download.assert_has_calls(
        [call('file.txt'), call('.file.txt.metadata')])


@pytest.mark.parametrize(
    'to_touch',
    [
        [Path('remote', 'file.txt')],
        [],
    ],
    ids=['product-exists', 'none-exists'],
)
def test_do_not_download_if_metadata_does_not_exist_in_remote(
        to_touch, tmp_directory):
    client = LocalStorageClient('remote', path_to_project_root='.')
    client.download = Mock(wraps=client.download)
    client._remote_exists = Mock(wraps=client._remote_exists)

    for f in to_touch:
        f.touch()

    dag = DAG(executor=Serial(build_in_subprocess=False))
    product = File('file.txt', client=client)
    PythonCallable(_touch, product, dag=dag)

    dag.build()

    # should call this to verify if the remote metadata exists
    client._remote_exists.assert_called_with(Path('.file.txt.metadata'))
    # should not attempt to download
    client.download.assert_not_called()


def test_do_not_download_if_file_does_not_exist_in_remote(tmp_directory):
    client = LocalStorageClient('remote', path_to_project_root='.')
    client.download = Mock(wraps=client.download)
    client._remote_exists = Mock(wraps=client._remote_exists)

    Path('remote', '.file.txt.metadata').touch()

    dag = DAG(executor=Serial(build_in_subprocess=False))
    product = File('file.txt', client=client)
    PythonCallable(_touch, product, dag=dag)

    dag.build()

    # should call twice, since metadata exists, it has to also check the file
    client._remote_exists.assert_has_calls(
        [call(Path('.file.txt.metadata')),
         call(Path('file.txt'))])
    # should not attempt to download
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


# NOTE: the following tests check File behavior when a DAG is executed,
# depending on dag.executor configuration, tasks might run in a child process
# which causes the monkeypatch + Mock combination not to work correctly, to
# support testing, we implement a subclasses instead


class FileWithUploadCounter(File):

    def upload(self):
        if not Path('.file.txt.metadata').exists():
            # we have to ensure File.upload is called after saving metadata
            # is there a better way to do this? Embedding this test checking
            # logic here doesn't seem right
            raise ValueError(
                'By the time File.upload is called, metadata must exist')

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

    def _is_outdated(self, outdated_by_code=True):
        return TaskStatus.WaitingDownload


@pytest.mark.parametrize('executor', [
    Serial(build_in_subprocess=False),
    Serial(build_in_subprocess=True),
    Parallel(),
])
def test_upload_after_dag_build(tmp_directory, executor):
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
def test_upload_error(executor, tmp_directory):
    dag = DAG(executor=executor)

    product = FileWithUploadError('file.txt')
    PythonCallable(_touch, product, dag=dag)

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    assert 'upload failed!' in str(excinfo.getrepr())
    assert dag['_touch'].exec_status == TaskStatus.Errored


class MyFakeClient:

    def _remote_exists(self, path):
        return True

    def close(self):
        pass

    def download(self, local, destination=None):
        pass


@pytest.mark.parametrize('executor', [
    Serial(build_in_subprocess=False),
    Serial(build_in_subprocess=True),
    Parallel(),
])
def test_download_error(executor, tmp_directory):
    dag = DAG(executor=executor)

    product = FileWithDownloadError('file.txt', client=MyFakeClient())
    PythonCallable(_touch, product, dag=dag)

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    assert 'download failed!' in str(excinfo.getrepr())
    assert dag['_touch'].exec_status == TaskStatus.Errored


def test_task_build_calls_download_if_remote_up_to_date_products(
        tmp_directory, monkeypatch):
    dag = _make_dag()
    dag.build()

    Path('file.txt').unlink()
    Path('root').unlink()
    dag = _make_dag()

    monkeypatch.setattr(File, 'download',
                        Mock(wraps=dag['root'].product.download))
    dag['root'].build()

    assert dag['root'].product.download.call_count == 1


def _make_dag_with_upstream():
    # run in the same process, to ensure the mock object is called
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.clients[File] = LocalStorageClient('remote', path_to_project_root='.')
    t1 = PythonCallable(_touch, File('1.txt'), dag=dag, name='root')
    PythonCallable(_touch, File('2.txt'), dag=dag, name=2)
    t3 = PythonCallable(_touch_upstream, File('3.txt'), dag=dag, name=3)
    t1 >> t3
    return dag


def test_task_build_raises_error_if_upstream_do_not_exist_in_remote(
        tmp_directory, monkeypatch):
    dag = _make_dag_with_upstream().render()

    with pytest.raises(TaskBuildError) as excinfo:
        dag[3].build()

    expected = ("Cannot build task 3 because the following upstream "
                "dependencies are missing: ['root']")
    assert expected in str(excinfo.value)


def test_task_build_downloads_upstream_if_up_to_date_then_executes(
        tmp_directory, monkeypatch):
    _make_dag_with_upstream().build()
    Path('1.txt').unlink()
    Path('3.txt').unlink()
    Path('remote/3.txt').unlink()
    dag = _make_dag_with_upstream().render()
    monkeypatch.setattr(dag['root'].product, 'download',
                        Mock(wraps=dag['root'].product.download))
    monkeypatch.setattr(dag[3].product, 'download',
                        Mock(wraps=dag[3].product.download))

    dag[3].build()

    # this should be downloaded
    assert dag['root'].product.download.call_count == 1
    # this should now be downloaded but executed
    dag[3].product.download.assert_not_called() == 1


def test_task_build_downloads_if_upstream_up_to_date_and_remote_up_to_date(
        tmp_directory, monkeypatch):
    _make_dag_with_upstream().build()
    Path('3.txt').unlink()
    dag = _make_dag_with_upstream().render()
    monkeypatch.setattr(File, 'download', Mock(wraps=dag[3].product.download))

    dag[3].build()

    assert dag[3].product.download.call_count == 1


def test_dag_build_calls_download_if_remote_up_to_date_products(
        tmp_directory, monkeypatch):
    dag = _make_dag()
    dag.build()

    Path('file.txt').unlink()
    Path('root').unlink()
    dag = _make_dag()

    monkeypatch.setattr(dag['root'].product, 'download',
                        Mock(wraps=dag['root'].product.download))
    monkeypatch.setattr(dag['task'].product, 'download',
                        Mock(wraps=dag['task'].product.download))

    dag.build()

    assert dag['root'].product.download.call_count == 1
    assert dag['task'].product.download.call_count == 1


def test_keeps_waiting_download_status_after_downloading_upstream_dependency(
        tmp_directory):
    dag = _make_dag(with_client=True)
    dag.build()
    Path('root').unlink()
    Path('file.txt').unlink()
    dag = _make_dag().render()
    dag['root'].build()

    assert dag['task'].exec_status == TaskStatus.WaitingDownload


def test_downloads_task_with_upstream_after_full_build_and_skips_after_it(
        tmp_directory):
    dag = _make_dag(with_client=True)
    dag.build()
    Path('root').unlink()
    Path('file.txt').unlink()
    dag = _make_dag().render()

    dag['task'].build()

    assert _make_dag().render()['task'].exec_status == TaskStatus.Skipped


def test_check_remote_status(tmp_directory):
    dag = _make_dag(with_client=True)
    root = dag['root'].product
    task = dag['task'].product

    assert root._is_remote_outdated(outdated_by_code=True)
    assert task._is_remote_outdated(outdated_by_code=True)

    dag.build()

    dag = _make_dag(with_client=True)
    root = dag['root'].product
    task = dag['task'].product

    assert not root._is_remote_outdated(outdated_by_code=True)
    assert not task._is_remote_outdated(outdated_by_code=True)

    Path('remote', 'root').unlink()
    dag = _make_dag(with_client=True)
    root = dag['root'].product
    task = dag['task'].product

    assert root._is_remote_outdated(outdated_by_code=True)
    assert task._is_remote_outdated(outdated_by_code=True)
