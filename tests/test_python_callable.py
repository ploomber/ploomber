import pytest
from pathlib import Path
from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.exceptions import DAGBuildError


class MyException(Exception):
    pass


def fn(product, a):
    Path(str(product)).write_text('things')


def fn_w_exception(product):
    raise MyException


def test_params_are_accesible_after_init():
    dag = DAG()
    t = PythonCallable(fn, File('file.txt'), dag, 'callable',
                       params=dict(a=1))
    assert t.params == dict(a=1)


def test_upstream_and_me_are_added():
    dag = DAG()
    product = File('file.txt')
    t = PythonCallable(fn, product, dag, 'callable',
                       params=dict(a=1))
    dag.render()

    assert t.params['product'] is product


def test_can_execute_python_callable(tmp_directory):
    dag = DAG()
    PythonCallable(fn, File('file.txt'), dag, 'callable',
                   params=dict(a=1))
    assert dag.build()


def test_exceptions_are_raised_with_serial_executor():
    dag = DAG()
    PythonCallable(fn_w_exception, File('file.txt'),
                   dag, 'callable')

    with pytest.raises(DAGBuildError):
        dag.build()
