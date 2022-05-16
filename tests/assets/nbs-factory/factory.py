# Content of factory.py
from ploomber import DAG
from ploomber.tasks import NotebookRunner
from pathlib import Path
import tempfile
from ploomber.products import File


def get_data(product):
    Path(product).write_text("hello")


def make():
    dag = DAG()

    NotebookRunner(Path('clean.py'),
                   File(Path(tempfile.mkdtemp()) / 'file.html'),
                   dag=dag)

    return dag
