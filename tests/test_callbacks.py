from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber.dag import DAG
from ploomber.tasks import PythonCallable, SQLScript
from ploomber.products import File
from ploomber.exceptions import DAGBuildError, CallbackSignatureError

# TODO: and wih the subprocess options on/off
# TODO: update other test aswell that parametrize on execuors to use all serial with subprocess on/off
# TODO: checking signature checks when adding callbacks


def fn(product):
    Path(str(product)).touch()


def fn_that_fails(product):
    raise Exception


def on_finish(task, client):
    print('on finish')


def on_failure(task, client):
    print('on failure')


def on_failure_crashing():
    raise Exception


def on_render(task, client):
    print('on_render')


def test_on_finish_wrong_signature():

    def on_finish(unknown_arg):
        pass

    dag = DAG()
    t = PythonCallable(fn, File('file1.txt'), dag)

    with pytest.raises(CallbackSignatureError):
        t.on_finish = on_finish


@pytest.mark.parametrize('executor', ['serial', 'parallel'])
def test_runs_on_finish(executor, tmp_directory, capsys):
    dag = DAG(executor=executor)
    t = PythonCallable(fn, File('file1.txt'), dag)
    t.on_finish = on_finish

    dag.build()

    assert capsys.readouterr().out == 'on finish\n'


@pytest.mark.parametrize('executor', ['serial', 'parallel'])
def test_runs_on_render(executor, tmp_directory, capsys):
    dag = DAG(executor=executor)
    t = PythonCallable(fn, File('file1.txt'), dag)
    t.on_render = on_render

    dag.build()

    assert capsys.readouterr().out == 'on_render\n'


@pytest.mark.parametrize('executor', ['serial', 'parallel'])
def test_runs_on_failure(executor, tmp_directory, capsys):
    dag = DAG(executor=executor)
    t = PythonCallable(fn_that_fails, File('file1.txt'), dag)
    t.on_failure = on_failure

    try:
        dag.build()
    except DAGBuildError:
        pass

    assert capsys.readouterr().out == 'on failure\n'


def test_warns_on_failure_crashing(tmp_directory):
    dag = DAG()
    t = PythonCallable(fn_that_fails, File('file1.txt'), dag)
    t.on_failure = on_failure_crashing

    with pytest.warns(UserWarning) as record:
        try:
            dag.build()
        except DAGBuildError:
            pass

    expected = 'Exception when running on_failure for task "fn_that_fails"'
    assert len(record) == 1
    assert expected in record[0].message.args[0]

# TODO: test warning is shown if on_finish crashes, same with on_failure
# TODO: DAGBuildError fs any on_finish crashes (all errors should be visible)
