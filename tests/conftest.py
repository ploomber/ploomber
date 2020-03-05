import string
import random
import json
from os import environ
import shutil
import os
import pytest
from pathlib import Path
import tempfile
from ploomber.clients import SQLAlchemyClient
from ploomber import Env
import pandas as pd


def _path_to_tests():
    return Path(__file__).absolute().parent


@pytest.fixture(scope='session')
def path_to_tests():
    return _path_to_tests()


@pytest.fixture()
def tmp_directory():
    old = os.getcwd()
    tmp = tempfile.mkdtemp()
    os.chdir(tmp)

    yield tmp

    os.chdir(old)


@pytest.fixture()
def sqlite_client_and_tmp_dir():
    """
    Creates a sqlite db with sample data and yields initialized client
    along with a temporary directory location
    """
    tmp_dir = Path(tempfile.mkdtemp())
    client = SQLAlchemyClient('sqlite:///' + str(tmp_dir / 'my_db.db'))
    df = pd.DataFrame({'x': range(10)})
    df.to_sql('data', client.engine)
    yield client, tmp_dir
    client.close()


@pytest.fixture
def cleanup_env():
    Env.end()
    yield None
    Env.end()


@pytest.fixture()
def tmp_intermediate_example_directory():
    """Move to examples/pipeline/
    """
    old = os.getcwd()
    path = _path_to_tests() / '..' / 'examples' / 'pipeline' / 'intermediate'
    tmp = Path(tempfile.mkdtemp()) / 'content'

    # we have to add extra folder content/, otherwise copytree complains
    shutil.copytree(path, tmp)
    os.chdir(tmp)

    yield tmp

    os.chdir(old)


@pytest.fixture()
def tmp_example_pipeline_directory():
    """Move to examples/pipeline/
    """
    old = os.getcwd()
    path = _path_to_tests() / '..' / 'examples' / 'pipeline'
    tmp = Path(tempfile.mkdtemp()) / 'content'

    # we have to add extra folder content/, otherwise copytree complains
    shutil.copytree(path, tmp)
    os.chdir(tmp)

    yield tmp

    os.chdir(old)


@pytest.fixture(scope='session')
def move_to_sample_dir():
    old = os.getcwd()
    new = _path_to_tests() / 'assets' / 'sample_dir'
    os.chdir(new)

    yield new

    os.chdir(old)


@pytest.fixture(scope='session')
def move_to_sample_subdir():
    old = os.getcwd()
    new = _path_to_tests() / 'assets' / 'sample_dir' / 'subdir'
    os.chdir(new)

    yield new

    os.chdir(old)


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
    p = Path('~', '.auth', 'postgres-ploomber.json').expanduser()

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

    yield client, schema

    # clean up schema
    client.execute('drop schema {} cascade;'.format(schema))

    client.close()


@pytest.fixture(scope='session')
def fake_conn():
    o = object()

    yield o
