import subprocess
import sqlite3
import pickle
import copy
from pathlib import Path
from unittest.mock import MagicMock, Mock

from subprocess import CalledProcessError

import sqlalchemy
import paramiko
import pytest

from ploomber import DAG
from ploomber.tasks import ShellScript
from ploomber.products import File
from ploomber.clients import (ShellClient, SQLAlchemyClient, DBAPIClient,
                              RemoteShellClient)
from ploomber.clients import db, shell

_SQLALCHEMY_ARGS = [
    sqlalchemy.engine.url.URL.create(drivername='sqlite', database='my_db.db'),
    'sqlite:///my_db.db',
]


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


@pytest.mark.parametrize('arg', _SQLALCHEMY_ARGS)
def test_deepcopy_sqlalchemyclient(tmp_directory, arg):
    client = SQLAlchemyClient(arg)
    client.execute('CREATE TABLE my_table (num INT)')
    assert copy.deepcopy(client)


@pytest.mark.parametrize('arg', _SQLALCHEMY_ARGS)
def test_pickle_sqlalchemyclient(tmp_directory, arg):
    client = SQLAlchemyClient(arg)
    client.execute('CREATE TABLE my_table (num INT)')
    assert pickle.dumps(client)


def test_creates_relative_dir_sqlalchemyclient(tmp_directory):
    intermediate_path = "a/relative/path"
    client = SQLAlchemyClient(f'sqlite:///{intermediate_path}/my_db.db')
    client.execute('CREATE TABLE my_table (num INT)')
    assert Path(tmp_directory + "/" + intermediate_path).exists()


def test_creates_absolute_dir_sqlalchemyclient(tmp_directory):
    intermediate_path = Path(tmp_directory, "an/absolute/path")
    client = SQLAlchemyClient(f'sqlite:///{intermediate_path}/my_db.db')
    client.execute('CREATE TABLE my_table (num INT)')
    assert intermediate_path.exists()


def test_does_not_create_in_memory_sqlalchemyclient(tmp_directory):
    client = SQLAlchemyClient('sqlite://')
    client.execute('CREATE TABLE my_table (num INT)')
    # Assert no folder/file was created in the temporary folder:
    assert next(Path(tmp_directory).iterdir(), None) is None


def test_custom_create_engine_kwargs(monkeypatch):

    mock = Mock()
    monkeypatch.setattr(db.sqlalchemy, 'create_engine', mock)
    client = SQLAlchemyClient('sqlite:///my_db.db',
                              create_engine_kwargs=dict(key='value'))

    # trigger call to create_engine
    client.engine

    mock.assert_called_once_with('sqlite:///my_db.db', key='value')


def test_init_sqlalhemy_with_url_object(tmp_directory):
    client = SQLAlchemyClient(
        sqlalchemy.engine.url.URL.create(drivername='sqlite',
                                         database='my_db.db'))

    client.execute('CREATE TABLE my_table (num INT)')
    assert Path('my_db.db').is_file()


@pytest.mark.parametrize(
    'code,split_source',
    [['CREATE TABLE my_table (num INT); SELECT * FROM my_table', 'default'],
     ['CREATE TABLE my_table (num INT); SELECT * FROM my_table', ';'],
     ['CREATE TABLE my_table (num INT)## SELECT * FROM my_table', '##']])
def test_send_more_than_one_command_in_sqlite(code, split_source,
                                              tmp_directory):
    client = SQLAlchemyClient('sqlite:///my_db.db', split_source=split_source)
    client.execute(code)


@pytest.mark.parametrize('arg', [
    sqlalchemy.engine.url.URL.create(drivername='postgresql',
                                     database='db',
                                     password='password',
                                     username='user',
                                     host='ploomber.io'),
    'postgresql://user:password@ploomber.io/db',
])
def test_sqlalchemy_client_repr_does_not_expose_password(arg):
    client = SQLAlchemyClient(arg)

    assert 'password' not in str(client)
    assert 'password' not in repr(client)


# TODO: some of the following tests no longer need tmp_directory because
# they use mock and files are no longer created
@pytest.mark.parametrize('run_template', [None, 'ruby {{path_to_code}}'])
def test_shell_client_execute(run_template, tmp_directory, monkeypatch):
    if run_template:
        client = ShellClient(run_template=run_template)
        expected_command = run_template.split(' ')[0]
    else:
        client = ShellClient()
        expected_command = 'bash'

    code = """
    echo 'hello'
    """

    mock_res = Mock()
    mock_res.returncode = 0
    mock_run_call = Mock(return_value=mock_res)

    monkeypatch.setattr(shell.subprocess, 'run', mock_run_call)
    # prevent tmp file from being removed so we can check contents
    monkeypatch.setattr(shell.Path, 'unlink', Mock())

    client.execute(code)

    cmd, path = mock_run_call.call_args[0][0]

    assert cmd == expected_command
    assert Path(path).read_text() == code


def test_shell_client_tmp_file_is_deleted(tmp_directory, monkeypatch):
    client = ShellClient()
    code = """
    echo 'hello'
    """
    mock_unlink = Mock()
    monkeypatch.setattr(shell.Path, 'unlink', mock_unlink)
    mock_res = Mock()
    mock_res.returncode = 0
    mock_run_call = Mock(return_value=mock_res)
    monkeypatch.setattr(shell.subprocess, 'run', mock_run_call)

    client.execute(code)

    mock_unlink.assert_called_once()


def test_task_level_shell_client(tmp_directory, monkeypatch):
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

    mock = Mock(wraps=client.execute)
    monkeypatch.setattr(client, 'execute', mock)

    mock_res = Mock()
    mock_res.returncode = 0

    def side_effect(*args, **kwargs):
        Path('a_file').touch()
        return mock_res

    mock_run_call = Mock(side_effect=side_effect)
    monkeypatch.setattr(shell.subprocess, 'run', mock_run_call)
    # prevent tmp file from being removed so we can check contents
    monkeypatch.setattr(shell.Path, 'unlink', Mock())

    dag.build()

    mock.assert_called_once()

    cmd, path_arg = mock_run_call.call_args[0][0]
    kwargs = mock_run_call.call_args[1]

    expected_code = """
    require 'fileutils'
    FileUtils.touch "{path}"
    """.format(path=path)

    assert cmd == 'ruby'
    assert Path(path_arg).read_text() == expected_code
    assert kwargs == {
        'stderr': subprocess.PIPE,
        'stdout': subprocess.PIPE,
        'shell': False
    }


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


def test_remote_shell_read_file(monkeypatch, tmp_directory):
    mock_ssh_client = Mock(spec=paramiko.SSHClient)
    monkeypatch.setattr(shell, 'ssh_client_and_policy', lambda:
                        (mock_ssh_client, Mock()))
    monkeypatch.setattr(shell.tempfile, 'mkstemp', lambda:
                        (None, 'my_tmp_file'))
    monkeypatch.setattr(shell.os, 'close', lambda _: None)
    mock_ssh_client.open_sftp().get.side_effect = lambda x, y: Path(
        'my_tmp_file').write_text('some content')
    # reset to prevent counting the "call" from the previous line
    mock_ssh_client.open_sftp.reset_mock()

    client = RemoteShellClient(connect_kwargs={}, path_to_directory='/tmp')

    returned_content = client.read_file('/path/to/remote/file')

    assert returned_content == 'some content'
    mock_ssh_client.open_sftp.assert_called_once()
    ftp = client.connection.open_sftp()
    assert ftp.get.call_args.assert_called_with('/path/to/remote/file',
                                                'my_tmp_file')
    ftp.close.assert_called_once()


def test_remote_shell_write_to_file(monkeypatch, tmp_directory):
    mock_ssh_client = Mock(spec=paramiko.SSHClient)
    monkeypatch.setattr(shell, 'ssh_client_and_policy', lambda:
                        (mock_ssh_client, Mock()))
    monkeypatch.setattr(shell.tempfile, 'mkstemp', lambda:
                        (None, 'my_tmp_file'))
    monkeypatch.setattr(shell.os, 'close', lambda _: None)
    monkeypatch.setattr(shell.Path, 'unlink', lambda _: None)

    client = RemoteShellClient(connect_kwargs={}, path_to_directory='/tmp')

    client.write_to_file('content', '/path/to/remote/file')

    mock_ssh_client.open_sftp.assert_called_once()
    ftp = client.connection.open_sftp()
    assert Path('my_tmp_file').read_text() == 'content'
    assert ftp.put.call_args.assert_called_with('my_tmp_file',
                                                '/path/to/remote/file')
    ftp.close.assert_called_once()
