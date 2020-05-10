from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import PythonCallable, SQLScript, SQLTransfer, SQLUpload
from ploomber.products import GenericProduct, GenericSQLRelation


params = [(GenericProduct, 'some_identifier.txt'),
          (GenericSQLRelation, ('a_table', 'table'))]


def touch(product):
    Path(str(product)).touch()


@pytest.mark.parametrize("class_,identifier", params)
def test_exists_sqlite_backend(sqlite_client_and_tmp_dir, class_, identifier):
    client, tmp_dir = sqlite_client_and_tmp_dir
    product = class_(identifier, client=client)
    assert not product.exists()


@pytest.mark.parametrize("class_,identifier", params)
def test_save_metadata_sqlite_backend(sqlite_client_and_tmp_dir, class_,
                                      identifier):
    client, tmp_dir = sqlite_client_and_tmp_dir
    product = GenericProduct('some_identifier.txt', client=client)
    m = {'metadata': 'value'}
    product.save_metadata(m)

    assert product.exists()
    assert product.fetch_metadata() == m


@pytest.mark.parametrize("class_,identifier", params)
def test_delete_sqlite_backend(sqlite_client_and_tmp_dir, class_, identifier):
    client, tmp_dir = sqlite_client_and_tmp_dir
    product = GenericProduct('some_identifier.txt', client=client)
    m = {'metadata': 'value'}
    product.save_metadata(m)
    product.delete()

    assert not product.exists()


@pytest.mark.parametrize("class_,identifier", params)
def test_sample_dag(sqlite_client_and_tmp_dir, class_, identifier):
    client, _ = sqlite_client_and_tmp_dir
    dag = DAG()
    product = GenericProduct('some_file.txt', client=client)
    PythonCallable(touch, product, dag)
    dag.build()

    assert Path('some_file.txt').exists()
    assert product.exists()
    assert product.fetch_metadata() is not None


@pytest.mark.parametrize("class_,source",
                         [(SQLScript,
                           'CREATE TABLE {{product}} as SELECT * FROM table'),
                          (SQLTransfer, '/some/file'),
                          (SQLUpload, '/some/file')])
def test_sql_with_sql_tasks(class_, source):
    dag = DAG()
    client_metadata = object()
    client = object()
    product = GenericSQLRelation(('name', 'table'), client=client_metadata)
    class_(source,
           product, dag, client=client, name='task')


def test_sql_stores_metadata_by_schema_and_name(sqlite_client_and_tmp_dir):
    client, tmp_dir = sqlite_client_and_tmp_dir
    dag = DAG()

    product = GenericSQLRelation(('schema', 'name', 'table'), client=client)
    product2 = GenericSQLRelation(('schema2', 'name', 'table'), client=client)

    PythonCallable(touch, product, dag, 't1')
    PythonCallable(touch, product2, dag, 't2')
    dag.build()

    # delete this product (will delete metadata)
    product.delete()

    # but should not affect this other one
    assert product2.exists()
