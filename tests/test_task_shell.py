from pathlib import Path
from unittest.mock import Mock

import pytest

from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import ShellScript
from ploomber.exceptions import SourceInitializationError


def test_build_task(tmp_directory, monkeypatch):
    dag = DAG()
    task = ShellScript('touch {{product}}',
                       File('file.txt'),
                       dag,
                       name='touch')

    # need this to because dag.build verifies products exist after execution
    def side_effect(code):
        Path('file.txt').touch()

    # mock the actual execution to make this test work on windows
    mock_execute = Mock(side_effect=side_effect)
    monkeypatch.setattr(task.client, 'execute', mock_execute)

    dag.build()

    mock_execute.assert_called_once_with('touch file.txt')


def test_error_if_missing_product(tmp_directory):
    dag = DAG()

    with pytest.raises(SourceInitializationError) as excinfo:
        ShellScript('touch file.txt', File('file.txt'), dag, name='touch')

    assert ('ShellScript must include {{product}} in its source'
            in str(excinfo.value))
