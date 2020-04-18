from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import Link, PythonCallable
from ploomber.products import File
from ploomber.exceptions import DAGRenderError


def touch_root(product):
    Path(str(product)).touch()


def touch(product, upstream):
    Path(str(product)).touch()


def test_link_is_up_to_date_before_build(tmp_directory):
    dag = DAG()

    Path('some_file').touch()
    t1 = Link(File('some_file'), dag, name='some_file')

    assert not t1.product._is_outdated()


def test_downstream_from_link_is_up_to_date_after_build(tmp_directory):
    # Link.metadata.timestamp is patched to return 0, hence checking timestamps
    # from upstream dependencies in t2 should not mark it as outdated
    dag = DAG()

    Path('some_file').touch()
    t1 = Link(File('some_file'), dag, name='some_file')
    t2 = PythonCallable(touch, File('another_file'), dag)
    t1 >> t2

    dag.build()

    assert not t2.product._is_outdated()


def test_error_raised_if_link_has_upstream_dependencies(tmp_directory):
    dag = DAG()

    t0 = PythonCallable(touch_root, File('another_file'), dag)
    t1 = Link(File('some_file'), dag, name='some_file')
    t0 >> t1

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    msg = ('Link tasks should not have upstream dependencies. '
           '"some_file" task has them')
    assert msg in str(excinfo.getrepr())


def test_error_raised_if_link_product_does_not_exist(tmp_directory):
    dag = DAG()

    Link(File('some_file'), dag, name='some_file')

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    msg = ('Link tasks should point to Products that already exist. '
           '"some_file" task product "some_file" does not exist')
    assert msg in str(excinfo.getrepr())
