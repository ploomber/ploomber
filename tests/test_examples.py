"""
Runs examples form examples/
"""
import subprocess


def test_pipeline_runs(tmp_intermediate_example_directory):
    assert subprocess.call(['python', 'pipeline.py']) == 0


def test_partitioned_execution_runs(tmp_example_pipeline_directory):
    assert subprocess.call(['python', 'partitioned_execution.py']) == 0
