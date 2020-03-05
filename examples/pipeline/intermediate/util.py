import json
from pathlib import Path


def load_db_uri():
    try:
        p = Path('~', '.auth', 'postgres-ploomber.json').expanduser()

        with open(p) as f:
            db = json.load(f)
    except FileNotFoundError:
        db = {
            'uri': 'postgresql://localhost:5432/db',
            'dbname': 'db',
            'host': 'localhost',
            'user': 'postgres',
            'password': '',
        }

    return db['uri']
