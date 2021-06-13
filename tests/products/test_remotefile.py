from pathlib import Path
import json
from unittest.mock import Mock

import pytest

from ploomber import DAG
from ploomber.executors import Serial
from ploomber.tasks import PythonCallable
from ploomber.clients import LocalStorageClient
from ploomber.products import File
from ploomber.products._remotefile import _RemoteFile


def _touch(product):
    Path(product).touch()


def _touch_upstream(product, upstream):
    Path(product).touch()


def _load_json(path):
    return json.loads(Path(path).read_text())


def _write_json(obj, path):
    with open(path, 'w') as f:
        json.dump(obj, f)


@pytest.fixture
def dag():
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.clients[File] = LocalStorageClient('remote', path_to_project_root='.')
    root = PythonCallable(_touch, File('root'), dag=dag, name='root')
    task = PythonCallable(_touch_upstream, File('file'), dag=dag, name='task')
    root >> task
    return dag


def test_metadata_is_equal_to_local_copy(tmp_directory, dag):
    dag.build()

    file_ = dag['task'].product
    rf = _RemoteFile(file_=file_)

    assert rf._is_equal_to_local_copy()


def test_is_not_outdated_after_build(tmp_directory, dag):
    dag.build()

    file_ = dag['task'].product
    rf = _RemoteFile(file_=file_)

    assert not rf._outdated_data_dependencies(with_respect_to_local=True)
    assert not rf._outdated_code_dependency()
    assert not rf._is_outdated(with_respect_to_local=True)


def test_is_outdated_due_data(tmp_directory, dag):
    dag.build()

    # modify metadata to make it look older
    meta = _load_json('remote/.file.metadata')
    meta['timestamp'] = 0
    _write_json(meta, 'remote/.file.metadata')

    file_ = dag['task'].product
    rf = _RemoteFile(file_=file_)

    assert rf._outdated_data_dependencies(with_respect_to_local=True)
    assert rf._is_outdated(with_respect_to_local=True)


def test_is_outdated_due_code(tmp_directory, dag):
    dag.build()

    # modify metadata to make the code look outdated
    meta = _load_json('remote/.file.metadata')
    meta['stored_source_code'] = meta['stored_source_code'] + '1+1\n'
    _write_json(meta, 'remote/.file.metadata')

    file_ = dag['task'].product
    rf = _RemoteFile(file_=file_)

    assert rf._outdated_code_dependency()
    assert rf._is_outdated(with_respect_to_local=True)


def test_caches_result(tmp_directory, monkeypatch, dag):
    dag.build()

    file_ = dag['task'].product
    rf = _RemoteFile(file_=file_)

    mock_check = Mock(wraps=rf._check_is_outdated)
    monkeypatch.setattr(rf, '_check_is_outdated', mock_check)

    # call a first time
    rf._is_outdated(with_respect_to_local=True)

    # this call shouldn't call _check_is_outdated
    rf._is_outdated(with_respect_to_local=True)

    mock_check.assert_called_once()


def test_deletes_metadata_file_after_init(tmp_directory, monkeypatch, dag):
    dag.build()

    file_ = dag['task'].product
    _RemoteFile(file_=file_)

    assert not Path('.file.metadata.remote').exists()


def test_calls_client_download_lazily(tmp_directory, monkeypatch, dag):
    dag.build()

    file_ = dag['task'].product

    download_mock = Mock(wraps=file_.client.download)
    monkeypatch.setattr(file_.client, 'download', download_mock)

    rf = _RemoteFile(file_=file_)

    download_mock.assert_not_called()

    assert rf.metadata.stored_source_code is not None
    assert rf.metadata.timestamp is not None
    assert rf.metadata.params is not None

    download_mock.assert_called_once()
