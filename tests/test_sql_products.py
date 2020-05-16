from datetime import datetime

from ploomber import DAG
from ploomber.tasks import SQLScript
from ploomber.products import SQLiteRelation, PostgresRelation

import pandas as pd
import numpy as np
import pytest


@pytest.fixture(params=['sqlite', 'pg'])
def client_and_prod(request, sqlite_client_and_tmp_dir, pg_client_and_schema):
    # Based on: https://github.com/pytest-dev/pytest/issues/349#issue-88534390
    if request.param == 'sqlite':
        client, _ = sqlite_client_and_tmp_dir
        product = SQLiteRelation((None, 'numbers', 'table'), client)
        schema = None
    else:
        client, schema = pg_client_and_schema
        product = PostgresRelation((schema, 'numbers', 'table'), client)

    yield client, product, schema

    product.delete()


def add_number_one(metadata):
    metadata['number'] = 1
    return metadata


def test_exists(client_and_prod):
    client, product, schema = client_and_prod
    product.render({})
    product.delete()

    assert not product.exists()

    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', client.engine, if_exists='replace', schema=schema)

    assert product.exists()


def test_delete(client_and_prod):
    client, product, schema = client_and_prod
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', client.engine, if_exists='replace', schema=schema)

    product.render({})
    product.delete()

    assert not product.exists()


def test_fetch_metadata_none_if_not_exists(client_and_prod):
    client, product, schema = client_and_prod
    product.render({})

    assert product.fetch_metadata() is None


def test_fetch_metadata_none_if_empty_metadata(client_and_prod):
    client, product, schema = client_and_prod
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', client.engine, if_exists='replace', schema=schema)
    product.render({})

    assert product.fetch_metadata() is None


def test_save_metadata(client_and_prod):
    client, product, schema = client_and_prod
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', client.engine, if_exists='replace', schema=schema)

    product.render({})
    metadata_new = {'timestamp': datetime.now().timestamp(),
                    'stored_source_code': 'some code'}

    product.save_metadata(metadata_new)

    fetched = product.fetch_metadata()

    assert fetched == metadata_new


def test_add_metadata_fields(client_and_prod):
    client, product, schema = client_and_prod
    dag = DAG()
    dag.clients[SQLScript] = client
    dag.clients[type(product)] = client

    query = 'CREATE TABLE {{product}} AS SELECT * FROM data'
    product.prepare_metadata = add_number_one

    SQLScript(query, product, dag, name='t1')

    dag.build()

    metadata = product.fetch_metadata()

    assert metadata['number'] == 1


@pytest.mark.parametrize('class_', [SQLiteRelation, PostgresRelation])
def test_error_message_when_missing_client(class_):
    # this works since we haven't tried to access the client yet...
    product = class_((None, 'name', 'table'))

    # this fails
    with pytest.raises(ValueError) as excinfo:
        product.client

    assert 'Cannot obtain client for this product' in str(excinfo.value)


# TODO: we should really be testing the mixin, which is where this logic
# is implemented
@pytest.mark.parametrize('arg', [(None, 'table', 'table'),
                                 ('schema', 'table', 'table')])
def test_old_metadata_is_replaced(arg, sqlite_client_and_tmp_dir):
    client, tmp = sqlite_client_and_tmp_dir
    schema, name, _ = arg
    product = SQLiteRelation(arg, client=client)

    product.render({})

    product.save_metadata({'timestamp': datetime.now().timestamp(),
                           'stored_source_code': 'some code'})

    product.save_metadata({'timestamp': datetime.now().timestamp(),
                           'stored_source_code': 'some code'})

    query = "SELECT COUNT(*) FROM _metadata WHERE name='{}'".format(name)

    if schema is not None:
        query += " AND schema='{}'".format(schema)

    result = list(client.engine.execute(query))[0][0]

    assert result == 1
