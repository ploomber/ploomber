import string
import json
from sqlite3 import connect
from pathlib import Path
from unittest.mock import Mock

from ploomber import DAG
from ploomber.tasks import SQLDump, SQLTransfer, SQLScript, PythonCallable
from ploomber.products import File, SQLiteRelation
from ploomber.clients import SQLAlchemyClient, DBAPIClient
from ploomber.executors import Serial
from ploomber import io

import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def sample_data():
    conn = connect('database.db')
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn, index=False)
    yield 'database.db'
    conn.close()


@pytest.mark.parametrize('client', [
    SQLAlchemyClient('sqlite:///database.db'),
    DBAPIClient(connect, dict(database='database.db'))
])
def test_sqlscript_load(tmp_directory, sample_data, client):

    dag = DAG()

    dag.clients[SQLScript] = client
    dag.clients[SQLiteRelation] = client

    SQLScript('CREATE TABLE {{product}} AS SELECT * FROM numbers',
              SQLiteRelation((None, 'another', 'table')),
              dag=dag,
              name='task')

    dag.build(close_clients=False)

    df = dag['task'].load()

    dag.close_clients()

    assert df.to_dict(orient='list') == {
        'a': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        'b': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
    }


def test_sqldump_does_not_required_product_tag(tmp_directory):
    tmp = Path(tmp_directory)

    # create a db
    conn = connect(str(tmp / "database.db"))
    client = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))
    # dump output path
    out = tmp / 'dump'

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)

    # create the task and run it
    dag = DAG()

    # pass template SQL code so it's treated as a placeholder, this will force
    # the render step
    SQLDump('SELECT * FROM numbers LIMIT {{limit}}',
            File(out),
            dag,
            name='dump.csv',
            client=client,
            chunksize=None,
            io_handler=io.CSVIO,
            params={'limit': 10})

    dag.render()


def test_can_dump_sqlite_to_csv(tmp_directory):
    tmp = Path(tmp_directory)

    # create a db
    conn = connect(str(tmp / "database.db"))
    client = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))
    # dump output path
    out = tmp / 'dump'

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)

    # create the task and run it
    dag = DAG()
    SQLDump('SELECT * FROM numbers',
            File(out),
            dag,
            name='dump.csv',
            client=client,
            chunksize=None,
            io_handler=io.CSVIO)

    dag.build()

    # load dumped data and data from the db
    dump = pd.read_csv(out)
    db = pd.read_sql_query('SELECT * FROM numbers', conn)

    conn.close()

    # make sure they are the same
    assert dump.equals(db)


def test_sqldump_with_dbapiclient(tmp_directory):
    client = DBAPIClient(connect, dict(database='my_db.db'))

    # make some data and save it in the db
    con_raw = connect(database='my_db.db')
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', con_raw)

    # create the task and run it
    dag = DAG()
    SQLDump('SELECT * FROM numbers',
            File('dump.csv'),
            dag,
            name='dump',
            client=client,
            chunksize=None,
            io_handler=io.CSVIO)

    dag.build()

    # load dumped data and data from the db
    dump = pd.read_csv('dump.csv')
    db = pd.read_sql_query('SELECT * FROM numbers', con_raw)

    client.close()
    con_raw.close()

    assert dump.equals(db)


def test_can_dump_sqlite_to_parquet(tmp_directory):
    tmp = Path(tmp_directory)

    # create a db
    conn = connect(str(tmp / "database.db"))
    client = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))
    # dump output path
    out = tmp / 'dump'

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)

    cur = conn.cursor()
    cur.execute('select * from numbers')

    # create the task and run it
    dag = DAG()
    SQLDump('SELECT * FROM numbers',
            File(out),
            dag,
            name='dump',
            client=client,
            chunksize=10,
            io_handler=io.ParquetIO)
    dag.build()

    # load dumped data and data from the db
    dump = pd.read_parquet(out)
    db = pd.read_sql_query('SELECT * FROM numbers', conn)

    conn.close()

    # make sure they are the same
    assert dump.equals(db)


# FIXME: add tests to check that Placeholders init from raw str, or a loader
# have the custom globals
def test_custom_jinja_env_globals(tmp_directory):
    tmp = Path(tmp_directory)

    # create a db
    conn = connect(str(tmp / "database.db"))
    client = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))

    # make some data and save it in the db
    df = pd.DataFrame({
        'number': range(10),
        'char': list(string.ascii_letters[:10])
    })
    df.to_sql('data', conn)

    # create the task and run it
    dag = DAG(executor=Serial(build_in_subprocess=False))

    t1 = SQLDump('SELECT * FROM data',
                 File('data.parquet'),
                 dag,
                 name='data',
                 client=client,
                 chunksize=None,
                 io_handler=io.ParquetIO)

    def select(product, upstream):
        numbers = list(pd.read_parquet(upstream['data']).number)
        numbers_selected = [n for n in numbers if n % 2 == 0]

        chars = list(pd.read_parquet(upstream['data']).char)
        chars_selected = [repr(c) for i, c in enumerate(chars) if i % 2 == 0]

        Path(product).write_text(
            json.dumps(dict(numbers=numbers_selected, chars=chars_selected)))

    t2 = PythonCallable(select, File('selected.json'), dag, name='selected')

    t3 = SQLDump("""
    -- {{upstream}}
    SELECT * FROM data WHERE number
    NOT IN ([[get_key(upstream["selected"], "numbers") | join(", ") ]])
""",
                 File('numbers.parquet'),
                 dag,
                 name='numbers',
                 client=client,
                 chunksize=None,
                 io_handler=io.ParquetIO)

    t4 = SQLDump("""
    -- {{upstream}}
    SELECT * FROM data WHERE char
    NOT IN ([[get_key(upstream["selected"], "chars") | join(", ") ]])
""",
                 File('chars.parquet'),
                 dag,
                 name='chars',
                 client=client,
                 chunksize=None,
                 io_handler=io.ParquetIO)

    t1 >> t2 >> t3
    t2 >> t4

    dag.build()

    assert list(pd.read_parquet('numbers.parquet').number) == [1, 3, 5, 7, 9]
    assert list(
        pd.read_parquet('chars.parquet').char) == ['b', 'd', 'f', 'h', 'j']


def test_can_dump_postgres(tmp_directory, pg_client_and_schema):
    pg_client, _ = pg_client_and_schema

    tmp = Path(tmp_directory)

    # dump output path
    out = tmp / 'dump'

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', pg_client.engine, if_exists='replace')

    # create the task and run it
    dag = DAG()
    SQLDump('SELECT * FROM numbers',
            File(out),
            dag,
            name='dump',
            client=pg_client,
            chunksize=10,
            io_handler=io.ParquetIO)
    dag.build()

    # load dumped data and data from the db
    dump = pd.read_parquet(out)
    db = pd.read_sql_query('SELECT * FROM numbers', pg_client.engine)

    # make sure they are the same
    assert dump.equals(db)


@pytest.mark.parametrize('product_arg, expected_io_handler', [
    ['out.csv', io.CSVIO],
    ['out.ext', io.CSVIO],
    ['{{some_placeholder}}/{{another_placeholder}}.csv', io.CSVIO],
    ['out.parquet', io.ParquetIO],
    ['{{some_placeholder}}/{{another_placeholder}}.parquet', io.ParquetIO],
])
def test_dump_io_handler(product_arg, expected_io_handler):

    dag = DAG()
    t = SQLDump('SELECT * FROM some_table',
                File(product_arg),
                dag,
                name='dump',
                client=Mock())

    assert expected_io_handler is t.io_handler


def test_can_transfer_sqlite(tmp_directory):
    tmp = Path(tmp_directory)

    # create clientections to 2 dbs
    client_in = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database_in.db"))
    client_out = SQLAlchemyClient('sqlite:///{}'.format(tmp /
                                                        "database_out.db"))

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', client_in.engine, index=False)

    # create the task and run it
    dag = DAG()
    SQLTransfer('SELECT * FROM numbers',
                SQLiteRelation((None, 'numbers2', 'table'), client=client_out),
                dag,
                name='transfer',
                client=client_in,
                chunksize=10)
    dag.build()

    # load dumped data and data from the db
    original = pd.read_sql_query('SELECT * FROM numbers', client_in.engine)
    transfer = pd.read_sql_query('SELECT * FROM numbers2', client_out.engine)

    client_in.close()
    client_out.close()

    # make sure they are the same
    assert original.equals(transfer)
