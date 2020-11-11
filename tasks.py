"""
Setup tasks (requires invoke: pip install invoke)
"""
from pathlib import Path
import base64
from invoke import task


@task
def db_credentials(c):
    """
    """
    path = str(Path('~', '.auth', 'postgres-ploomber.json').expanduser())
    creds = Path(path).read_text()
    print(base64.b64encode(creds.encode()).decode())
