from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber.dag import DAG
from ploomber.tasks import PythonCallable, SQLScript
from ploomber.products import File
from ploomber.exceptions import DAGBuildError

# TODO: test with all clients!
# TODO: and wih the subprocess options on/off
# TODO: update other test aswell to use all serial with subprocess on/off
# TODO: checking signature checks when adding callbacks


def fn1(product):
    Path(str(product)).touch()


def fn_that_fails(product):
    raise Exception


def on_finish(task, client):
    print('on finish')


def on_failure(task, client):
    print('on failure')


@pytest.mark.parametrize('executor', ['serial', 'parallel'])
def test_runs_on_finish(executor, tmp_directory, capsys):
    dag = DAG()
    t = PythonCallable(fn1, File('file1.txt'), dag, name='fn1')
    t.on_finish = on_finish

    dag.build()

    assert capsys.readouterr().out == 'on finish\n'


@pytest.mark.parametrize('executor', ['serial', 'parallel'])
def test_runs_on_failure(executor, tmp_directory, capsys):
    dag = DAG()
    t = PythonCallable(fn_that_fails, File('file1.txt'), dag)
    t.on_failure = on_failure

    try:
        dag.build()
    except DAGBuildError:
        pass

    assert capsys.readouterr().out == 'on failure\n'


# TODO: add on render
# TODO: test warning is shown if on_finish crashes
# TODO: DAGBuildError is any on_finish crashes (all errors should be visible)
