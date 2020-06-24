from pathlib import Path
import json


def get_uri():
    return 'sqlite:///my.db'


def get_pg_uri():
    p = str(Path('~', '.auth', 'postgres-ploomber.json').expanduser())

    try:
        with open(p) as f:
            uri = json.load(f)['uri']

    # if that does not work, try connecting to a local db (this is the
    # case when running on Travis)
    except FileNotFoundError:
        uri = 'postgresql://localhost:5432/db'

    return uri


def get_metadata_uri():
    return 'sqlite:///metadata.db'
