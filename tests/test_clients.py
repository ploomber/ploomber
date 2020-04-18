import sqlite3
import pickle
import copy
from pathlib import Path
from urllib.parse import urlparse

from ploomber import DAG
from ploomber.tasks import ShellScript
from ploomber.products import File
from ploomber.clients import ShellClient, SQLAlchemyClient, DBAPIClient
from ploomber.clients import db


def test_deepcopy_dbapiclient(tmp_directory):
    client = DBAPIClient(sqlite3.connect, dict(database='my_db.db'))
    client.execute('CREATE TABLE my_table (num INT)')
    assert copy.deepcopy(client)


def test_pickle_dbapiclient(tmp_directory):
    client = DBAPIClient(sqlite3.connect, dict(database='my_db.db'))
    client.execute('CREATE TABLE my_table (num INT)')
    assert pickle.dumps(client)


def test_deepcopy_sqlalchemyclient(tmp_directory):
    client = SQLAlchemyClient('sqlite:///my_db.db')
    client.execute('CREATE TABLE my_table (num INT)')
    assert copy.deepcopy(client)


def test_pickle_sqlalchemyclient(tmp_directory):
    client = SQLAlchemyClient('sqlite:///my_db.db')
    client.execute('CREATE TABLE my_table (num INT)')
    assert pickle.dumps(client)


def test_send_more_than_one_command_in_sqlite(tmp_directory):
    client = SQLAlchemyClient('sqlite:///my_db.db')
    code = """
    CREATE TABLE my_table (num INT);
    SELECT * FROM my_table
    """
    client.execute(code)


def test_safe_uri():
    # with password
    res = db.safe_uri(urlparse('postgresql://user:pass@localhost/db'))
    assert res == 'postgresql://user:********@localhost/db'

    # no password
    res = db.safe_uri(urlparse('postgresql://user@localhost/db'))
    assert res == 'postgresql://user@localhost/db'


def test_shell_client(tmp_directory):
    path = Path(tmp_directory, 'a_file')

    client = ShellClient()
    code = """
    touch a_file
    """
    assert not path.exists()

    client.execute(code)

    assert path.exists()


def test_shell_client_with_custom_template(tmp_directory):
    path = Path(tmp_directory, 'a_file')

    client = ShellClient(run_template='ruby {{path_to_code}}')
    code = """
    require 'fileutils'
    FileUtils.touch "a_file"
    """
    assert not path.exists()

    client.execute(code)

    assert path.exists()


def test_custom_client_in_dag(tmp_directory):
    path = Path(tmp_directory, 'a_file')

    dag = DAG()

    client = ShellClient(run_template='ruby {{path_to_code}}')

    dag.clients[ShellScript] = client

    ShellScript("""
    require 'fileutils'
    FileUtils.touch "{{product}}"
    """,
                product=File(path),
                dag=dag,
                name='ruby_script')

    assert not path.exists()

    dag.build()

    assert path.exists()


def test_db_code_split():
    assert list(db.code_split('a;b;c;')) == ['a', 'b', 'c']
    assert list(db.code_split('a;b;c;\n')) == ['a', 'b', 'c']
