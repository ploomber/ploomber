"""
Note: tests organized in folders must contain an init file:
https://github.com/pytest-dev/pytest/issues/3151#issuecomment-360493948
"""
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


@fixture_backup('simple')
def backup_simple():
    pass


@fixture_backup('online')
def backup_online():
    pass


@pytest.fixture()
def tmp_directory():
    old = os.getcwd()
    tmp = tempfile.mkdtemp()
    os.chdir(str(tmp))

    yield tmp

    # some tests create sample git repos, if we are on windows, we need to
    # change permissions to be able to delete the files
    if os.name == 'nt' and Path('.git').exists():
        for root, dirs, files in os.walk('.git'):
            for dir_ in dirs:
                os.chmod(Path(root, dir_), stat.S_IRWXU)
            for file_ in files:
                os.chmod(Path(root, file_), stat.S_IRWXU)

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


def _load_db_credentials():
    p = Path('~', '.auth', 'postgres-ploomber.json').expanduser()

    # if running locally, load from file
    if p.exists():
        with open(p) as f:
            db = json.load(f)

    # if no credentials file, use env variable (used in windows CI)
    elif 'POSTGRES' in os.environ:
        b64 = os.environ['POSTGRES']
        json_str = base64.b64decode(b64).decode()
        db = json.loads(json_str)

    # otherwise, use local posttgres db (used in linux CI)
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
