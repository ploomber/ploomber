import sqlite3
import pickle
import copy
from pathlib import Path

from ploomber import DAG
from ploomber.tasks import ShellScript
from ploomber.products import File
from ploomber.clients import ShellClient, SQLAlchemyClient, DBAPIClient


def test_deepcopy_dbapiclient(tmp_directory):
    client = DBAPIClient(sqlite3.connect, database='my_db.db')
    client.execute('CREATE TABLE my_table (num INT)')
    assert copy.deepcopy(client)


def test_pickle_dbapiclient(tmp_directory):
    client = DBAPIClient(sqlite3.connect, database='my_db.db')
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
