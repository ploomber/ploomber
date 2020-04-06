from pathlib import Path

from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import ShellScript


def test_build_task(tmp_directory):
    dag = DAG()
    ShellScript('touch {{product}}', File('file.txt'), dag, name='touch')
    dag.build()

    assert Path('file.txt').exists()
