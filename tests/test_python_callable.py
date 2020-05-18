import pytest
from pathlib import Path

from test_pkg import functions
from ploomber import DAG, DAGConfigurator
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.exceptions import DAGBuildError, TaskRenderError, DAGRenderError


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


def test_catches_signature_errors_at_render_time():
    dag = DAG()
    t = PythonCallable(fn, File('file.txt'), dag, 'callable',
                       params=dict(non_param=1))

    with pytest.raises(TaskRenderError):
        t.render()

    with pytest.raises(DAGRenderError):
        dag.render()


def test_hot_reload(backup_test_pkg, tmp_directory):
    cfg = DAGConfigurator()
    cfg.params.hot_reload = True
    dag = cfg.create()

    t1 = PythonCallable(functions.touch_root,
                        File('file1.txt'), dag)
    t2 = PythonCallable(functions.touch_upstream,
                        File('file2.txt'), dag)
    t1 >> t2

    # import pickle
    # from copy import deepcopy
    # from ploomber.sources import PythonCallableSource

    # t1.source._hot_reload = False

    # s = deepcopy(t1.source)
    # s._hot_reload = False
    # t1._source = s

    # pickle.dumps(t1)
    # pickle.dumps(t2)

    path_to_functions = Path(backup_test_pkg, 'functions.py')
    source_new = """
def touch_root(product):
    Path(str(product)).write_text("hi")

def touch_upstream(product, upstream):
    Path(str(product)).write_text("hello")
    """
    path_to_functions.write_text(source_new)

    # import types
    # def _reloaded(self):
    #     pass

    # import importlib
    # importlib.reload(functions)
    # t1_ = t1._source._primitive
    # t2_ = t2._source._primitive
    # t1._source = PythonCallableSource(t1_)
    # t2._source = PythonCallableSource(t2_)

    # import pickle

    # t1.source._primitive = None

    # pickle.dumps(t1.source._primitive_init)
    
    # t1.source._primitive_init

    # print('SOURCE', path_to_functions.read_text())

    # t1.source._reloaded = types.MethodType(_reloaded, t1)
    # t2.source._reloaded = types.MethodType(_reloaded, t2)
    # t1.source._hot_reload = False
    # t2.source._hot_reload = False
    dag.build()

    assert Path('file1.txt').read_text() == 'hi'
    assert Path('file2.txt').read_text() == 'hello'
