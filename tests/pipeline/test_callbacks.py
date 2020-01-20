from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber.dag import DAG
from ploomber.tasks import BashCommand, PythonCallable, SQLScript
from ploomber.products import File


def fn1(product):
    Path(str(product)).touch()


def fn_that_fails(product):
    raise Exception


def on_finish(task):
    print('running on finish')


def on_finish_w_client(task, client):
    pass


def on_failure(task, traceback):
    print('running on failure')


def on_failure_w_client(task, client):
    pass


def test_runs_on_finish(tmp_directory, capsys):

    dag = DAG()
    t = PythonCallable(fn1, File('file1.txt'), dag, name='fn1')
    t.on_finish = on_finish
    dag.build()

    assert capsys.readouterr().out == 'running on finish\n'


def test_passes_client_to_on_finish(tmp_directory):

    dag = DAG()
    t = PythonCallable(fn1, File('file1.txt'), dag, name='fn1')
    t.on_finish = on_finish_w_client
    dag.build()


# def test_runs_on_failure(tmp_directory, capsys):

#     dag = DAG()
#     t = PythonCallable(fn_that_fails, File('file1.txt'), dag)
#     t.on_failure = on_failure
#     dag.build()

#     assert capsys.readouterr().out == 'running on failure\n'


# def test_passes_client_to_on_failure(tmp_directory):

#     dag = DAG()
#     t = PythonCallable(fn1, File('file1.txt'), dag)
#     t.on_finish = on_finish_w_client
#     dag.build()
