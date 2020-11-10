import json
from pathlib import Path


def load_db_uri():
    try:
        p = str(Path('~', '.auth', 'postgres-ploomber.json').expanduser())

        with open(p) as f:
            db = json.load(f)
    except FileNotFoundError:
        db = {
            'uri': 'postgresql://postgres:postgres@localhost:5432/postgres',
            'dbname': 'postgres',
            'host': 'localhost',
            'user': 'postgres',
            'password': 'postgres',
        }

    return db['uri']
