from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import ShellScript
from ploomber.exceptions import SourceInitializationError


def test_build_task(tmp_directory):
    dag = DAG()
    ShellScript('touch {{product}}', File('file.txt'), dag, name='touch')
    dag.build()

    assert Path('file.txt').exists()


def test_error_if_missing_product(tmp_directory):
    dag = DAG()

    with pytest.raises(SourceInitializationError) as excinfo:
        ShellScript('touch file.txt', File('file.txt'), dag, name='touch')

    assert ('ShellScript must include {{product}} in its source'
            in str(excinfo.value))
