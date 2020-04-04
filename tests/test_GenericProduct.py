from pathlib import Path

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import GenericProduct


def touch(product):
    Path(str(product)).touch()


def test_exists_sqlite_backend(sqlite_client_and_tmp_dir):
    client, tmp_dir = sqlite_client_and_tmp_dir
    product = GenericProduct('some_identifier', client=client)
    assert not product.exists()


def test_save_metadata_sqlite_backend(sqlite_client_and_tmp_dir):
    client, tmp_dir = sqlite_client_and_tmp_dir
    product = GenericProduct('some_identifier', client=client)
    m = {'metadata': 'value'}
    product.save_metadata(m)

    assert product.exists()
    assert product.fetch_metadata() == m


def test_delete_sqlite_backend(sqlite_client_and_tmp_dir):
    client, tmp_dir = sqlite_client_and_tmp_dir
    product = GenericProduct('some_identifier', client=client)
    m = {'metadata': 'value'}
    product.save_metadata(m)
    product.delete()

    assert not product.exists()


def test_sample_dag(sqlite_client_and_tmp_dir):
    client, tmp_dir = sqlite_client_and_tmp_dir
    dag = DAG()
    product = GenericProduct('some_file', client=client)
    PythonCallable(touch, product, dag)
    dag.build()

    assert Path('some_file').exists()
    assert product.exists()
    assert product.fetch_metadata() is not None
