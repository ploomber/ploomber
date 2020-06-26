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
    return Path(__file__).absolute().parent


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
            tmp = Path(tmp_dir,  'content')
            # we have to add extra folder content/, otherwise copytree
            # complains
            shutil.copytree(str(source), str(tmp))
            os.chdir(str(tmp))
            yield tmp
            os.chdir(old)
            shutil.rmtree(tmp_dir)

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
    backup = tempfile.mkdtemp()
    root = Path(test_pkg.__file__).parents[2]

    # sanity check, in case we change the structure
    assert root.name == 'test_pkg'

    shutil.copytree(str(root), str(Path(backup, 'test_pkg')))

    yield str(Path(importlib.util.find_spec('test_pkg').origin).parent)

    shutil.rmtree(str(root))
    shutil.copytree(str(Path(backup, 'test_pkg')), str(root))
    shutil.rmtree(backup)


@pytest.fixture()
def tmp_directory():
    old = os.getcwd()
    tmp = tempfile.mkdtemp()
    os.chdir(str(tmp))

    yield tmp

    shutil.rmtree(str(tmp))
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
    shutil.rmtree(str(tmp_dir))
    client.close()


@pytest.fixture
def cleanup_env():
    Env.end()
    yield None
    Env.end()


@fixture_tmp_dir(_path_to_tests() / '..' / 'examples' / 'pipeline'
                 / 'intermediate')
def tmp_intermediate_example_directory():
    pass


@fixture_tmp_dir(_path_to_tests() / '..' / 'examples' / 'pipeline')
def tmp_example_pipeline_directory():
    pass


@fixture_tmp_dir(_path_to_tests() / '..' / 'recipes')
def tmp_recipes_directory():
    pass


@fixture_tmp_dir(_path_to_tests() / '..' / 'examples')
def tmp_examples_directory():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'sample_dir')
def tmp_sample_dir():
    pass


@pytest.fixture()
def tmp_sample_subdir():
    old = os.getcwd()
    tmp = Path(tempfile.mkdtemp(), 'sample_dir')
    sample_dir = _path_to_tests() / 'assets' / 'sample_dir'
    shutil.copytree(str(sample_dir), str(tmp))

    os.chdir(str(tmp / 'subdir'))

    yield tmp

    shutil.rmtree(str(tmp))
    os.chdir(old)


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'nbs')
def tmp_nbs():
    pass


@pytest.fixture(scope='session')
def path_to_source_code_file():
    return (_path_to_tests() / 'assets' / 'sample' /
            'src' / 'pkg' / 'module' / 'functions.py')


@pytest.fixture(scope='session')
def path_to_env():
    return _path_to_tests() / 'assets' / 'sample' / 'env.yaml'


@pytest.fixture(scope='session')
def path_to_assets():
    return _path_to_tests() / 'assets'


def _load_db_credentials():

    # try load credentials from a local file
    p = str(Path('~', '.auth', 'postgres-ploomber.json').expanduser())

    try:
        with open(p) as f:
            db = json.load(f)

    # if that does not work, try connecting to a local db (this is the
    # case when running on Travis)
    except FileNotFoundError:
        db = {
            'uri': 'postgresql://localhost:5432/db',
            'dbname': 'db',
            'host': 'localhost',
            'user': 'postgres',
            'password': '',
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

    # this client is only used to setup the schema and default schema
    # see this: https://www.postgresonline.com/article_pfriendly/279.html
    client_tmp = SQLAlchemyClient(db['uri'])

    # set a new schema for this session, otherwise if two test sessions
    # are run at the same time, tests might conflict with each other
    # NOTE: avoid upper case characters, pandas.DataFrame.to_sql does not like
    # them
    schema = (''.join(random.choice(string.ascii_letters)
                      for i in range(12))).lower()

    client_tmp.execute('CREATE SCHEMA {};'.format(schema))
    client_tmp.execute('ALTER USER "{}" SET search_path TO {};'
                       .format(db['user'], schema))
    client_tmp.close()

    client = SQLAlchemyClient(db['uri'])

    df = pd.DataFrame({'x': range(10)})
    df.to_sql('data', client.engine)

    yield client, schema

    # clean up schema
    client.execute('drop schema {} cascade;'.format(schema))

    client.close()


@pytest.fixture(scope='session')
def fake_conn():
    o = object()

    yield o


@pytest.fixture
def add_current_to_sys_path():
    old = copy(sys.path)
    sys.path.append('')
    yield sys.path
    sys.path = old
