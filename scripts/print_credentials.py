"""
Usage:
    python print_credentials.py | pbcopy

Print db credentials, save them in DB_CREDENTIALS env variable on Travis
"""
from pathlib import Path

p = Path('~', '.auth', 'postgres-ploomber.json').expanduser()

print(p.read_text().replace('\n', '').replace(' ', '')
      .replace('"', '\\"').replace(',', '\\,'))
