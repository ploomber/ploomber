"""
Runs examples form examples/
"""
import os
import pytest
import subprocess
from conftest import _path_to_tests


_examples = [f for f in os.listdir(_path_to_tests().parent / 'examples')
             if f.endswith('.py')]


def test_pipeline_runs(tmp_intermediate_example_directory):
    assert subprocess.call(['python', 'pipeline.py']) == 0


def test_partitioned_execution_runs(tmp_example_pipeline_directory):
    assert subprocess.call(['python', 'partitioned_execution.py']) == 0


@pytest.mark.parametrize('name', _examples)
def test_examples(tmp_examples_directory, name):
    # TODO: add timeout
    assert subprocess.call(['python', name]) == 0
