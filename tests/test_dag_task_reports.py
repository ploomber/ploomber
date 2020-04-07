from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber.dag import DAG
from ploomber.tasks import ShellScript, PythonCallable, SQLDump
from ploomber.products import File
from ploomber.constants import TaskStatus, DAGStatus
from ploomber.exceptions import DAGBuildError, DAGRenderError


def touch_root(product):
    Path(str(product)).touch()


def touch(upstream, product):
    Path(str(product)).touch()


# TODO: test dag.status(), task.status()

def test_task_report_after_building(tmp_directory):
    dag = DAG()

    t = PythonCallable(touch_root, File('some_file'), dag, name='task')

    t.render()
    report = t.build()

    assert report['Ran?']
    assert report['Elapsed (s)']
    assert report['name'] == 'task'


def test_dag_report_after_building(tmp_directory):
    dag = DAG()

    PythonCallable(touch_root, File('some_file'), dag, name='task')
    PythonCallable(touch_root, File('some_file'), dag, name='task2')

    report = dag.build()

    assert report['Ran?'] == [True, True]
    assert len(report['Elapsed (s)']) == 2
    assert len(report['name']) == 2
    assert len(report['Percentage']) == 2

    report = dag.build()

    assert report['Ran?'] == [False, False]
    assert len(report['Elapsed (s)']) == 2
    assert len(report['name']) == 2
    assert len(report['Percentage']) == 2
