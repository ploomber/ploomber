"""
Test PostgresCopy task
"""
import pandas as pd

from ploomber import DAG
from ploomber.products import PostgresRelation, File
from ploomber.tasks import PostgresCopy, PythonCallable


def test_can_upload_a_file(tmp_directory, pg_client_and_schema):
    pg_client, schema = pg_client_and_schema

    df = pd.DataFrame({'a': [1, 2, 3]})
    df.to_parquet('data.parquet')

    dag = DAG()

    dag.clients[PostgresRelation] = pg_client
    dag.clients[PostgresCopy] = pg_client

    PostgresCopy('data.parquet',
                 product=PostgresRelation((schema,
                                           'test_can_upload_a_file',
                                           'table')),
                 dag=dag,
                 name='upload')

    dag.build()


def test_can_upload_file_from_upstream_dependency(tmp_directory,
                                                  pg_client_and_schema):

    pg_client, schema = pg_client_and_schema

    def make_data(product):
        df = pd.DataFrame({'a': [1, 2, 3]})
        df.to_parquet(str(product))

    dag = DAG()

    dag.clients[PostgresRelation] = pg_client
    dag.clients[PostgresCopy] = pg_client

    make = PythonCallable(make_data,
                          product=File('data.parquet'),
                          dag=dag,
                          name='make')

    name = 'test_can_upload_file_from_upstream_dependency'
    pg = PostgresCopy('{{upstream["make"]}}',
                      product=PostgresRelation((schema,
                                                name,
                                                'table')),
                      dag=dag,
                      name='upload')

    make >> pg

    dag.build()
