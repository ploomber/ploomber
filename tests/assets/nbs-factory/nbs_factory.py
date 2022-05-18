# Content of factory.py
from ploomber import DAG
from ploomber.tasks import NotebookRunner
from pathlib import Path
import tempfile
from ploomber.products import File


def make():
    dag = DAG()

    NotebookRunner(Path('clean.py'),
                   File(Path(tempfile.mkdtemp()) / 'file.html'),
                   dag=dag,
                   name='clean')

    return dag
