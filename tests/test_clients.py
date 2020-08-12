import sqlite3
import pickle
import copy
from pathlib import Path
from urllib.parse import urlparse
from unittest.mock import MagicMock
from subprocess import CalledProcessError

import paramiko
import pytest

from ploomber import DAG
from ploomber.tasks import ShellScript
from ploomber.products import File
from ploomber.clients import (ShellClient, SQLAlchemyClient, DBAPIClient,
                              RemoteShellClient)
from ploomber.clients import db


def test_deepcopy_dbapiclient(tmp_directory):
    client = DBAPIClient(sqlite3.connect, dict(database='my_db.db'))
    client.execute('CREATE TABLE my_table (num INT)')
    assert copy.deepcopy(client)


def test_pickle_dbapiclient(tmp_directory):
    client = DBAPIClient(sqlite3.connect, dict(database='my_db.db'))
    client.execute('CREATE TABLE my_table (num INT)')
    assert pickle.dumps(client)


def test_dbapiclient_split_source(tmp_directory):
    client = DBAPIClient(sqlite3.connect,
                         dict(database='my_db.db'),
                         split_source=';')
    client.execute("""DROP TABLE IF EXISTS my_table;
    CREATE TABLE my_table (num INT)""")
    assert pickle.dumps(client)


def test_dbapiclient_split_source_custom_char(tmp_directory):
    client = DBAPIClient(sqlite3.connect,
                         dict(database='my_db.db'),
                         split_source='##')
    client.execute("""DROP TABLE IF EXISTS my_table##
    CREATE TABLE my_table (num INT)""")
    assert pickle.dumps(client)


def test_deepcopy_sqlalchemyclient(tmp_directory):
    client = SQLAlchemyClient('sqlite:///my_db.db')
    client.execute('CREATE TABLE my_table (num INT)')
    assert copy.deepcopy(client)


def test_pickle_sqlalchemyclient(tmp_directory):
    client = SQLAlchemyClient('sqlite:///my_db.db')
    client.execute('CREATE TABLE my_table (num INT)')
    assert pickle.dumps(client)


@pytest.mark.parametrize(
    'code,split_source',
    [['CREATE TABLE my_table (num INT); SELECT * FROM my_table', 'default'],
     ['CREATE TABLE my_table (num INT); SELECT * FROM my_table', ';'],
     ['CREATE TABLE my_table (num INT)## SELECT * FROM my_table', '##']])
def test_send_more_than_one_command_in_sqlite(code, split_source,
                                              tmp_directory):
    client = SQLAlchemyClient('sqlite:///my_db.db', split_source=split_source)
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


def test_remote_shell(monkeypatch):
    fake_client = MagicMock(spec=paramiko.SSHClient)
    stdout = MagicMock()
    stdout.readline = lambda: ''
    stdout.channel.recv_exit_status.return_value = 0
    fake_client.exec_command.return_value = 'input', stdout, 'err'
    sftp = MagicMock()
    fake_client.open_sftp.return_value = sftp

    monkeypatch.setattr(paramiko, 'SSHClient', lambda: fake_client)

    client = RemoteShellClient(connect_kwargs={}, path_to_directory='/tmp')
    client.execute('some code')

    fake_client.open_sftp.assert_called_once()
    fake_client.exec_command.assert_called_once()
    sftp.put.assert_called_once()
    sftp.close.assert_called_once()

    client.close()

    fake_client.close.assert_called_once()


def test_remote_shell_error(monkeypatch):
    fake_client = MagicMock(spec=paramiko.SSHClient)
    stdout = MagicMock()
    stdout.readline = lambda: ''
    stdout.channel.recv_exit_status.return_value = 1
    fake_client.exec_command.return_value = 'input', stdout, 'err'
    sftp = MagicMock()
    fake_client.open_sftp.return_value = sftp

    monkeypatch.setattr(paramiko, 'SSHClient', lambda: fake_client)
    client = RemoteShellClient(connect_kwargs={}, path_to_directory='/tmp')

    with pytest.raises(CalledProcessError):
        client.execute('some code')
