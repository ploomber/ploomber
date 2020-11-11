import os
import base64
import json
from pathlib import Path


def load_db_uri():
    p = str(Path('~', '.auth', 'postgres-ploomber.json').expanduser())

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

    return db['uri']
