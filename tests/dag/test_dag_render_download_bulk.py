"""
Tests remote metadata bulk download during DAG.render
"""

from glob import glob
from pathlib import Path
from unittest.mock import Mock

import pytest

from ploomber import DAG
from ploomber.products import File, SQLiteRelation
from ploomber.clients import LocalStorageClient, SQLAlchemyClient
from ploomber.tasks import PythonCallable, SQLScript
from ploomber.executors import Serial
from ploomber.dag import dag as dag_module
from ploomber.exceptions import DAGRenderError
from ploomber.products import file


def touch(product, upstream):
    Path(product).touch()


def touch_root(product):
    Path(product).touch()


def touch_root_metaproduct(product):
    for p in product:
        Path(p).touch()


@pytest.fixture
def dag():
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.clients[File] = LocalStorageClient("remote", path_to_project_root=".")
    PythonCallable(touch_root, File("one"), dag=dag)
    return dag


@pytest.fixture
def dag_w_error():
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.clients[File] = LocalStorageClient("remote", path_to_project_root=".")
    PythonCallable(touch_root, File("one"), dag=dag, params=dict(a=1))
    return dag


def test_calls_parallel_download(tmp_directory, monkeypatch, dag):
    dag.build()

    mock = Mock(wraps=dag_module.fetch_remote_metadata_in_parallel)
    monkeypatch.setattr(dag_module, "fetch_remote_metadata_in_parallel", mock)
    mock_remote = Mock()
    monkeypatch.setattr(file._RemoteFile, "_fetch_remote_metadata", mock_remote)

    dag.render()

    mock.assert_called_once_with(dag)
    # called once since this dag has a single product
    mock_remote.assert_called_once_with()
    # should clean up remote copies
    assert not glob("*.metadata.remote")


def test_cleans_up_files_if_render_fails(dag_w_error):
    with pytest.raises(DAGRenderError):
        dag_w_error.render()

    assert not glob("*.metadata.remote")


def test_ignores_non_file_products(tmp_directory, monkeypatch):
    mock_remote = Mock()
    monkeypatch.setattr(file._RemoteFile, "_fetch_remote_metadata", mock_remote)

    client = SQLAlchemyClient("sqlite:///my.db")

    dag = DAG()
    dag.clients[SQLScript] = client
    dag.clients[SQLiteRelation] = client
    dag.clients[File] = LocalStorageClient("remote", path_to_project_root=".")

    SQLScript(
        "CREATE TABLE {{product}} AS SELECT * FROM data",
        SQLiteRelation(["data2", "table"]),
        dag=dag,
        name="task",
    )

    dag.render()
    client.close()

    mock_remote.assert_not_called()


def test_processes_metaproducts(tmp_directory, monkeypatch):
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.clients[File] = LocalStorageClient("remote", path_to_project_root=".")
    PythonCallable(
        touch_root_metaproduct, {"one": File("one"), "two": File("two")}, dag=dag
    )
    dag.build()

    mock = Mock(wraps=dag_module.fetch_remote_metadata_in_parallel)
    monkeypatch.setattr(dag_module, "fetch_remote_metadata_in_parallel", mock)
    mock_remote = Mock()
    monkeypatch.setattr(file._RemoteFile, "_fetch_remote_metadata", mock_remote)

    dag.render()

    mock.assert_called_once_with(dag)
    assert mock_remote.call_count == 2
    # should clean up remote copies
    assert not glob("*.metadata.remote")


def test_dag_without_client(monkeypatch, tmp_directory):
    mock = Mock(wraps=dag_module.fetch_remote_metadata_in_parallel)
    monkeypatch.setattr(dag_module, "fetch_remote_metadata_in_parallel", mock)
    mock_remote = Mock()
    monkeypatch.setattr(file._RemoteFile, "_fetch_remote_metadata", mock_remote)

    dag = DAG(executor=Serial(build_in_subprocess=False))
    PythonCallable(touch_root, File("one"), dag=dag)

    dag.render()

    # should call it
    mock.assert_called_once_with(dag)
    # but should not call remotes
    mock_remote.assert_not_called()


def test_dag_with_files_and_metaproducts(monkeypatch, tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.clients[File] = LocalStorageClient("remote", path_to_project_root=".")
    t1 = PythonCallable(
        touch_root_metaproduct, {"one": File("one"), "two": File("two")}, dag=dag
    )
    t2 = PythonCallable(touch, File("three"), dag=dag)
    t1 >> t2

    dag.build()

    mock = Mock(wraps=dag_module.fetch_remote_metadata_in_parallel)
    monkeypatch.setattr(dag_module, "fetch_remote_metadata_in_parallel", mock)
    mock_remote = Mock()
    monkeypatch.setattr(file._RemoteFile, "_fetch_remote_metadata", mock_remote)

    dag.render()

    mock.assert_called_once_with(dag)
    assert mock_remote.call_count == 3
    assert not glob("*.metadata.remote")
