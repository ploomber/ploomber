import os
import base64
from pathlib import Path
import json

from ploomber.clients import SQLAlchemyClient


def get_uri():
    return 'sqlite:///my.db'


def get_client():
    return SQLAlchemyClient(get_uri())


def get_pg_client():
    p = str(Path('~', '.auth', 'postgres-ploomber.json').expanduser())

    # if running locally, load from file
    if p.exists():
        with open(p) as f:
            uri = json.load(f)['uri']

    # if no credentials file, use env variable (used in windows CI)
    elif 'POSTGRES' in os.environ:
        b64 = os.environ['POSTGRES']
        json_str = base64.b64decode(b64).decode()
        uri = json.loads(json_str)['uri']

    # otherwise, use local posttgres db (used in linux CI)
    else:
        uri = 'postgresql://postgres:postgres@localhost:5432/postgres'

    return SQLAlchemyClient(uri)


def get_metadata_client():
    return SQLAlchemyClient('sqlite:///metadata.db')
