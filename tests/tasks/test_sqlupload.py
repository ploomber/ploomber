"""
Test SQLUpload task
"""
from pathlib import Path
import pandas as pd

import pytest

from ploomber import DAG
from ploomber.products import (PostgresRelation, File, GenericSQLRelation,
                               SQLiteRelation)
from ploomber.tasks import SQLUpload, PythonCallable
from ploomber.clients import SQLAlchemyClient


def make_data(product):
    df = pd.DataFrame({'a': [1, 2, 3]})
    df.to_parquet(str(product))


@pytest.mark.parametrize('serializer, task_arg',
                         [('to_parquet', 'data.parquet'),
                          ('to_parquet', Path('data.parquet')),
                          ('to_csv', 'data.csv')])
def test_can_upload_a_file(serializer, task_arg, tmp_directory,
                           pg_client_and_schema):
    pg_client, schema = pg_client_and_schema

    df = pd.DataFrame({'a': [1, 2, 3]})
    getattr(df, serializer)(task_arg)

    dag = DAG()

    dag.clients[SQLUpload] = pg_client
    dag.clients[PostgresRelation] = pg_client

    SQLUpload(task_arg,
              product=PostgresRelation(
                  (schema, 'test_can_upload_a_file', 'table')),
              dag=dag,
              name='upload',
              to_sql_kwargs={'if_exists': 'replace'})

    dag.build()


@pytest.mark.parametrize('serializer, task_arg',
                         [('to_parquet', 'data.parquet'),
                          ('to_parquet', Path('data.parquet')),
                          ('to_csv', 'data.csv')])
def test_upload_a_file_with_generic_relation(serializer, task_arg,
                                             sqlite_client_and_tmp_dir,
                                             pg_client_and_schema):
    client, _ = sqlite_client_and_tmp_dir
    pg_client, schema = pg_client_and_schema

    df = pd.DataFrame({'a': [1, 2, 3]})
    getattr(df, serializer)(task_arg)

    dag = DAG()

    dag.clients[SQLUpload] = pg_client
    dag.clients[GenericSQLRelation] = client

    SQLUpload(task_arg,
              product=GenericSQLRelation(
                  (schema, 'test_can_upload_a_file', 'table')),
              dag=dag,
              name='upload',
              to_sql_kwargs={'if_exists': 'replace'})

    dag.build()


def test_append_rows(tmp_directory, pg_client_and_schema):
    pg_client, schema = pg_client_and_schema

    df = pd.DataFrame({'a': [1, 2, 3]})
    df.to_csv('data.csv', index=False)

    dag = DAG()

    dag.clients[SQLUpload] = pg_client
    dag.clients[PostgresRelation] = pg_client

    # create table
    df.to_sql('test_append',
              pg_client.engine,
              schema=schema,
              if_exists='replace',
              index=False)

    SQLUpload('data.csv',
              product=PostgresRelation((schema, 'test_append', 'table')),
              dag=dag,
              name='upload',
              to_sql_kwargs={
                  'if_exists': 'append',
                  'index': False
              })

    dag.build()

    df = pd.read_sql('SELECT * FROM {}.test_append'.format(schema),
                     pg_client.engine)

    assert df.shape[0] == 6


def test_can_upload_file_from_upstream_dependency(tmp_directory,
                                                  pg_client_and_schema):

    pg_client, schema = pg_client_and_schema

    dag = DAG()

    dag.clients[SQLUpload] = pg_client
    dag.clients[PostgresRelation] = pg_client

    make = PythonCallable(make_data,
                          product=File('data.parquet'),
                          dag=dag,
                          name='make')

    name = 'test_can_upload_file_from_upstream_dependency'
    pg = SQLUpload('{{upstream["make"]}}',
                   product=PostgresRelation((schema, name, 'table')),
                   dag=dag,
                   name='upload',
                   to_sql_kwargs={'if_exists': 'replace'})

    make >> pg

    dag.build()


def test_custom_io_handler(tmp_directory):
    dag = DAG()
    client = SQLAlchemyClient('sqlite:///database.db')
    dag.clients[SQLUpload] = client
    dag.clients[SQLiteRelation] = client

    df = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 3]})
    df.to_csv('some-file.tsv', sep='\t', index=False)

    def my_reading_fn(path):
        return pd.read_csv(path, sep='\t')

    SQLUpload('some-file.tsv',
              SQLiteRelation(('my-table', 'table')),
              dag=dag,
              name='task',
              io_handler=my_reading_fn,
              to_sql_kwargs=dict(index=False))

    dag.build()

    other = pd.read_sql('SELECT * FROM "my-table"', con=client)

    client.close()

    assert other.equals(df)
