from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import Input, PythonCallable
from ploomber.products import File
from ploomber.exceptions import DAGRenderError


def touch_root(product):
    Path(str(product)).touch()


def touch(product, upstream):
    Path(str(product)).touch()


def test_input_always_executes(tmp_directory):
    dag = DAG()

    Path('some_file.txt').touch()
    t1 = Input(File('some_file.txt'), dag, name='some_file')

    assert t1.product._is_outdated()

    dag.build()

    assert t1.product._is_outdated()


def test_error_raised_if_input_has_upstream_dependencies(tmp_directory):
    dag = DAG()

    t0 = PythonCallable(touch_root, File('another_file.txt'), dag)
    Path('some_file.txt').touch()
    t1 = Input(File('some_file.txt'), dag, name='some_file')

    with pytest.raises(RuntimeError) as excinfo:
        t0 >> t1

    msg = 'Input tasks should not have upstream dependencies'
    assert msg in str(excinfo.getrepr())


def test_error_raised_if_input_product_does_not_exist(tmp_directory):
    dag = DAG()

    with pytest.raises(RuntimeError) as excinfo:
        Input(File('some_file.txt'), dag, name='some_file')

    msg = ('Input tasks should point to Products that already exist. '
           '"some_file" task product "some_file.txt" does not exist')
    assert msg in str(excinfo.getrepr())
