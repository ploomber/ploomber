"""
Note: tests organized in folders must contain an init file:
https://github.com/pytest-dev/pytest/issues/3151#issuecomment-360493948
"""
import subprocess
import stat
import base64
from copy import copy
import sys
import importlib
import string
import random
import json
import shutil
import os
import pytest
from pathlib import Path
import tempfile
from functools import wraps
import test_pkg
from ploomber.clients import SQLAlchemyClient
from ploomber import Env
import pandas as pd
from glob import iglob
from ploomber.cli import install, examples
import posthog
from unittest.mock import Mock, MagicMock


def git_init():
    if Path('CHANGELOG.md').exists():
        raise ValueError('call git_init in a temporary directory')

    subprocess.run(['git', 'init', '-b', 'mybranch'])
    subprocess.check_call(['git', 'config', 'user.email', 'ci@ploomberio'])
    subprocess.check_call(['git', 'config', 'user.name', 'Ploomber'])

    subprocess.run(['git', 'add', '--all'])
    subprocess.run(['git', 'commit', '-m', 'some-commit-message'])


@pytest.fixture
def tmp_git(tmp_directory):
    Path('file').touch()
    git_init()


# FIXME: do we need this?
@pytest.fixture(scope='class')
def monkeypatch_session():
    from _pytest.monkeypatch import MonkeyPatch
    m = MonkeyPatch()
    yield m
    m.undo()


@pytest.fixture(scope='class', autouse=True)
def external_access(request, monkeypatch_session):
    # https://miguendes.me/pytest-disable-autouse
    if 'allow_posthog' in request.keywords:
        yield
    else:
        external_access = MagicMock()
        external_access.get_something = MagicMock(
            return_value='Mock was used.')
        monkeypatch_session.setattr(posthog, 'capture',
                                    external_access.get_something)


def _path_to_tests():
    return Path(__file__).resolve().parent


def fixture_tmp_dir(source):
    """
    A lot of our fixtures are copying a few files into a temporary location,
    making that location the current working directory and deleting after
    the test is done. This decorator allows us to build such fixture
    """

    # NOTE: I tried not making this a decorator and just do:
    # some_fixture = factory('some/path')
    # but didn't work
    def decorator(function):
        @wraps(function)
        def wrapper():
            old = os.getcwd()
            tmp_dir = tempfile.mkdtemp()
            tmp = Path(tmp_dir, 'content')
            # we have to add extra folder content/, otherwise copytree
            # complains
            shutil.copytree(str(source), str(tmp))
            os.chdir(str(tmp))
            yield tmp

            # some tests create sample git repos, if we are on windows, we
            # need to change permissions to be able to delete the files
            _fix_all_dot_git_permissions(tmp)

            os.chdir(old)
            shutil.rmtree(tmp_dir)

        return pytest.fixture(wrapper)

    return decorator


def fixture_backup(source):
    """
    Similar to fixture_tmp_dir but backups the content instead
    """
    def decorator(function):
        @wraps(function)
        def wrapper():
            old = os.getcwd()
            backup = tempfile.mkdtemp()
            root = _path_to_tests() / 'assets' / source
            shutil.copytree(str(root), str(Path(backup, source)))

            os.chdir(root)

            yield root

            os.chdir(old)

            shutil.rmtree(str(root))
            shutil.copytree(str(Path(backup, source)), str(root))
            shutil.rmtree(backup)

        return pytest.fixture(wrapper)

    return decorator


@pytest.fixture(scope='session')
def path_to_tests():
    return _path_to_tests()


@pytest.fixture()
def path_to_test_pkg():
    return str(Path(importlib.util.find_spec('test_pkg').origin).parent)


@pytest.fixture
def backup_test_pkg():
    old = os.getcwd()
    backup = tempfile.mkdtemp()
    root = Path(test_pkg.__file__).parents[2]

    # sanity check, in case we change the structure
    assert root.name == 'test_pkg'

    shutil.copytree(str(root), str(Path(backup, 'test_pkg')))

    yield str(Path(importlib.util.find_spec('test_pkg').origin).parent)
    os.chdir(old)

    shutil.rmtree(str(root))
    shutil.copytree(str(Path(backup, 'test_pkg')), str(root))
    shutil.rmtree(backup)


@fixture_backup('spec-with-functions')
def backup_spec_with_functions():
    pass


@fixture_backup('spec-with-functions-flat')
def backup_spec_with_functions_flat():
    pass


@fixture_backup('spec-with-functions-no-sources')
def backup_spec_with_functions_no_sources():
    pass


@fixture_backup('simple')
def backup_simple():
    pass


@fixture_backup('online')
def backup_online():
    pass


def _fix_dot_git_permissions(path):
    for root, dirs, files in os.walk(path):
        for dir_ in dirs:
            os.chmod(Path(root, dir_), stat.S_IRWXU)
        for file_ in files:
            os.chmod(Path(root, file_), stat.S_IRWXU)


def _fix_all_dot_git_permissions(tmp):
    if os.name == 'nt':
        for path in iglob(f'{tmp}/**/.git', recursive=True):
            _fix_dot_git_permissions(path)


@pytest.fixture()
def tmp_directory():
    old = os.getcwd()
    tmp = tempfile.mkdtemp()
    os.chdir(str(tmp))

    yield tmp

    # some tests create sample git repos, if we are on windows, we need to
    # change permissions to be able to delete the files
    _fix_all_dot_git_permissions(tmp)

    os.chdir(old)

    shutil.rmtree(str(tmp))


@pytest.fixture
def tmp_directory_local(tmp_path):
    """
    Pretty much the same as tmp_directory, but it uses pytest tmp_path,
    which creates the path in a pre-determined location depending on the test,
    TODO: replace the logic in tmp_directory with this one
    """
    old = os.getcwd()
    os.chdir(tmp_path)

    yield tmp_path

    os.chdir(old)


@pytest.fixture()
def sqlite_client_and_tmp_dir():
    """
    Creates a sqlite db with sample data and yields initialized client
    along with a temporary directory location
    """
    old = os.getcwd()
    tmp_dir = Path(tempfile.mkdtemp())
    os.chdir(str(tmp_dir))
    client = SQLAlchemyClient('sqlite:///' + str(tmp_dir / 'my_db.db'))
    df = pd.DataFrame({'x': range(10)})
    df.to_sql('data', client.engine)
    yield client, tmp_dir
    os.chdir(old)
    client.close()
    shutil.rmtree(str(tmp_dir))


@pytest.fixture
def cleanup_env():
    Env.end()
    yield None
    Env.end()


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'sample_dir')
def tmp_sample_dir():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'sample_tasks')
def tmp_sample_tasks():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'pipeline-sql')
def tmp_pipeline_sql():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'nbs')
def tmp_nbs():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'nbs-nested')
def tmp_nbs_nested():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'nbs-no-yaml')
def tmp_nbs_no_yaml():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'partial')
def tmp_partial():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'fns-and-scripts')
def tmp_fns_and_scripts():
    pass


@pytest.fixture(scope='session')
def path_to_source_code_file():
    return (_path_to_tests() / 'assets' / 'sample' / 'src' / 'pkg' / 'module' /
            'functions.py')


@pytest.fixture(scope='session')
def path_to_env():
    return _path_to_tests() / 'assets' / 'sample' / 'env.yaml'


@pytest.fixture(scope='session')
def path_to_assets():
    return _path_to_tests() / 'assets'


def _write_sample_conda_env(name='environment.yml', env_name='my_tmp_env'):
    Path(name).write_text(f'name: {env_name}\ndependencies:\n- pip')


def _write_sample_conda_env_lock():
    _write_sample_conda_env(name='environment.lock.yml')


def _write_sample_conda_files(dev=False):
    _write_sample_conda_env(
        'environment.yml' if not dev else 'environment.dev.yml')
    _write_sample_conda_env(
        'environment.lock.yml' if not dev else 'environment.dev.lock.yml')


def _write_sample_pip_files(dev=False):
    Path('requirements.txt' if not dev else 'requirements.dev.txt').touch()
    Path('requirements.lock.txt' if not dev else 'requirements.dev.lock.txt'
         ).touch()


def _write_sample_pip_req(name='requirements.txt'):
    Path(name).touch()


def _write_sample_pip_req_lock(name='requirements.lock.txt'):
    Path(name).touch()


def _prepare_files(
    has_conda,
    use_lock,
    env,
    env_lock,
    reqs,
    reqs_lock,
    monkeypatch,
):
    mock = Mock(return_value=has_conda)
    monkeypatch.setattr(install.shutil, 'which', mock)

    if env:
        _write_sample_conda_env()

    if env_lock:
        _write_sample_conda_env_lock()

    if reqs:
        _write_sample_pip_req()

    if reqs_lock:
        _write_sample_pip_req_lock()


def _load_db_credentials():
    p = Path('~', '.auth', 'postgres-ploomber.json').expanduser()

    # if running locally, load from file
    if p.exists():
        with open(p) as f:
            db = json.load(f)

    # if no credentials file, use env variable (previously, this was the method
    # used for running the Windows and macOS CI, but we not updated them to
    # use a local db)
    elif 'POSTGRES' in os.environ:
        b64 = os.environ['POSTGRES']
        json_str = base64.b64decode(b64).decode()
        db = json.loads(json_str)

    # otherwise, use local db
    else:
        db = {
            'uri': 'postgresql://postgres:postgres@localhost:5432/postgres',
            'dbname': 'postgres',
            'host': 'localhost',
            'user': 'postgres',
            'password': 'postgres',
        }

    return db


@pytest.fixture
def db_credentials():
    return _load_db_credentials()


@pytest.fixture(scope='session')
def pg_client_and_schema():
    """
    Creates a temporary schema for the testing session, drops everything
    at the end
    """
    db = _load_db_credentials()

    # set a new schema for this session, otherwise if two test sessions
    # are run at the same time, tests might conflict with each other
    # NOTE: avoid upper case characters, pandas.DataFrame.to_sql does not like
    # them
    schema = (''.join(random.choice(string.ascii_letters)
                      for i in range(12))).lower()

    # initialize client, set default schema
    # info: https://www.postgresonline.com/article_pfriendly/279.html
    client = SQLAlchemyClient(db['uri'],
                              create_engine_kwargs=dict(connect_args=dict(
                                  options=f'-c search_path={schema}')))

    # create schema
    client.execute('CREATE SCHEMA {};'.format(schema))

    df = pd.DataFrame({'x': range(10)})
    df.to_sql('data', client.engine)

    yield client, schema

    # clean up schema
    client.execute('DROP SCHEMA {} CASCADE;'.format(schema))
    client.close()


@pytest.fixture(scope='session')
def fake_conn():
    o = object()

    yield o


@pytest.fixture
def add_current_to_sys_path():
    old = copy(sys.path)
    sys.path.insert(0, os.path.abspath('.'))
    yield sys.path
    sys.path = old


@pytest.fixture
def no_sys_modules_cache():
    """
    Removes modules from sys.modules that didn't exist before the test
    """
    mods = set(sys.modules)

    yield

    current = set(sys.modules)

    to_remove = current - mods

    for a_module in to_remove:
        del sys.modules[a_module]


@pytest.fixture
def tmp_imports(add_current_to_sys_path, no_sys_modules_cache):
    """
    Adds current directory to sys.path and deletes everything imported during
    test execution upon exit
    """
    yield


@pytest.fixture
def _mock_email(monkeypatch):
    examples_email_mock = Mock()
    monkeypatch.setattr(examples, '_email_input', examples_email_mock)
