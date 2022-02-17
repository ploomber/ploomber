import logging
from pathlib import Path

import pytest

from tests_util import executors_w_exception_logging
from ploomber import DAG
from ploomber.tasks import PythonCallable, SQLScript
from ploomber.products import File, SQLiteRelation
from ploomber.exceptions import (DAGBuildError, CallbackSignatureError,
                                 DAGRenderError)
from ploomber.executors import Serial
from ploomber.constants import TaskStatus

# TODO: and wih the subprocess options on/off
# TODO: update other test aswell that parametrize on execuors to use all
# serial with subprocess on/off
# add to test dag : test counts in sucessful run, build again and counts
# are the same
# TODO: tests when on_render/on_finish fail an then on_failure fails
# TODO: test callbacks when calling a task directly render and build,
# TODO: in test_tasks test task status when build/render task directly
# status to other tasks shoould propagate as well


def fn(product):
    if hasattr(fn, 'count'):
        fn.count += 1
    Path(str(product)).touch()


def touch_w_upstream(product, upstream):
    if hasattr(touch_w_upstream, 'count'):
        touch_w_upstream.count += 1
    Path(str(product)).touch()


def fail_w_upstream(product, upstream):
    if hasattr(fail_w_upstream, 'count'):
        fail_w_upstream.count += 1
    raise Exception


def fn_that_fails(product):
    if hasattr(fn_that_fails, 'count'):
        fn_that_fails.count += 1
    raise Exception


def hook():
    if hasattr(hook, 'count'):
        hook.count += 1


def hook_2():
    if hasattr(hook_2, 'count'):
        hook_2.count += 1


def hook_3():
    if hasattr(hook_3, 'count'):
        hook_3.count += 1


def hook_4():
    if hasattr(hook_4, 'count'):
        hook_4.count += 1


def hook_crashing():
    if hasattr(hook_crashing, 'count'):
        hook_crashing.count += 1
    raise Exception


_executors = [
    Serial(build_in_subprocess=False, catch_exceptions=False),
    Serial(build_in_subprocess=False, catch_exceptions=True),
    Serial(build_in_subprocess=True, catch_exceptions=False),
    Serial(build_in_subprocess=True, catch_exceptions=True), 'parallel'
]

_executors_catch_exc = [
    Serial(build_in_subprocess=False, catch_exceptions=True),
    Serial(build_in_subprocess=True, catch_exceptions=True), 'parallel'
]

_executors_serial_catch_exc = [
    Serial(build_in_subprocess=False, catch_exceptions=True),
    Serial(build_in_subprocess=True, catch_exceptions=True)
]

_executors_current_process = [
    Serial(build_in_subprocess=False, catch_exceptions=False),
    Serial(build_in_subprocess=False, catch_exceptions=True)
]


@pytest.mark.parametrize('executor', _executors_current_process)
def test_runs_on_finish(executor, tmp_directory):
    hook.count = 0
    hook_2.count = 0
    hook_3.count = 0
    hook_4.count = 0

    dag = DAG(executor=executor)
    t = PythonCallable(fn, File('file1.txt'), dag, 't')
    t.on_finish = hook
    t.on_failure = hook_4

    t2 = PythonCallable(touch_w_upstream, File('file2'), dag, 't2')
    t2.on_finish = hook_2

    t3 = PythonCallable(fn, File('file3'), dag, 't3')
    t3.on_finish = hook_3

    t >> t2

    dag.build()

    assert hook.count == 1
    assert hook_2.count == 1
    assert hook_3.count == 1
    assert hook_4.count == 0


@pytest.mark.parametrize('executor', _executors_current_process)
@pytest.mark.parametrize('method', ['build', 'render'])
def test_runs_on_render(executor, method, tmp_directory):
    hook.count = 0
    hook_2.count = 0
    hook_3.count = 0

    dag = DAG(executor=executor)
    t = PythonCallable(fn, File('file1.txt'), dag, 't')
    t.on_render = hook

    t2 = PythonCallable(touch_w_upstream, File('file2'), dag, 't2')
    t2.on_render = hook_2

    t3 = PythonCallable(fn, File('file3'), dag, 't3')
    t3.on_render = hook_3

    t >> t2

    getattr(dag, method)()

    assert hook.count == 1
    assert hook_2.count == 1
    assert hook_3.count == 1


def test_runs_on_failure(tmp_directory):
    hook.count = 0
    hook_2.count = 0
    hook_3.count = 0

    dag = DAG(
        executor=Serial(build_in_subprocess=False, catch_exceptions=True))
    t = PythonCallable(fn_that_fails, File('file1.txt'), dag, 't')
    t.on_failure = hook
    t2 = PythonCallable(fn_that_fails, File('file2'), dag, 't2')
    t2.on_failure = hook_2
    t2 = PythonCallable(fn_that_fails, File('file3'), dag, 't3')
    t2.on_failure = hook_3

    try:
        dag.build()
    except DAGBuildError:
        pass

    assert hook.count == 1
    assert hook_2.count == 1
    assert hook_3.count == 1


########################
# Logging hook crashes #
########################


@pytest.mark.parametrize('executor', executors_w_exception_logging)
def test_on_render_exceptions_are_logged(executor, caplog):
    dag = DAG(executor=executor)
    t = PythonCallable(fn, File('file.txt'), dag, name='t')
    t.on_render = hook_crashing

    with caplog.at_level(logging.ERROR):
        with pytest.raises(DAGRenderError):
            dag.render()

    assert "Exception when running on_render for task 't'" in caplog.text


@pytest.mark.parametrize('executor', executors_w_exception_logging)
def test_on_finish_exceptions_are_logged(executor, tmp_directory, caplog):
    dag = DAG(executor=executor)
    t = PythonCallable(fn, File('file.txt'), dag, name='t')
    t.on_finish = hook_crashing

    with caplog.at_level(logging.ERROR):
        with pytest.raises(DAGBuildError):
            dag.build()

    assert "Exception when running on_finish for task 't'" in caplog.text


@pytest.mark.parametrize('executor', executors_w_exception_logging)
def test_on_failure_exceptions_are_logged(executor, caplog):
    dag = DAG(executor='serial')
    t = PythonCallable(fn_that_fails, File('file.txt'), dag, name='t')
    t.on_failure = hook_crashing

    with caplog.at_level(logging.ERROR):
        with pytest.raises(DAGBuildError):
            dag.build()

    assert "Exception when running on_failure for task 't'" in caplog.text


# TODO: parametrize by executor since reported status depends on it


@pytest.mark.parametrize('method', ['render', 'build'])
def test_task_status_when_on_render_crashes(method, tmp_directory):
    dag = DAG()
    t = PythonCallable(fn, File('file'), dag)
    t.on_render = hook_crashing
    t2 = PythonCallable(touch_w_upstream, File('file2'), dag)
    t >> t2

    with pytest.raises(DAGRenderError) as excinfo:
        getattr(dag, method)()

    assert t.exec_status == TaskStatus.ErroredRender
    assert t2.exec_status == TaskStatus.AbortedRender
    assert "PythonCallable: fn -> File('file')" in str(excinfo.getrepr())

    # let's also check we can recover
    t.on_render = hook

    assert getattr(dag, method)()


# TODO: parametrize by executor since reported status depends on it


def test_task_status_and_output_when_on_finish_crashes(tmp_directory):
    dag = DAG()
    t = PythonCallable(fn, File('file'), dag)
    t.on_finish = hook_crashing
    t2 = PythonCallable(touch_w_upstream, File('file2'), dag)
    t >> t2

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    assert t.exec_status == TaskStatus.Errored
    assert t2.exec_status == TaskStatus.Aborted
    assert "PythonCallable: fn -> File('file')" in str(excinfo.getrepr())


# TODO: parametrize by executor since reported status depends on it
def test_task_status_and_output_when_on_failure_crashes(tmp_directory):
    dag = DAG()
    t = PythonCallable(fn_that_fails, File('file'), dag)
    t.on_failure = hook_crashing

    t2 = PythonCallable(touch_w_upstream, File('file2'), dag)
    t >> t2

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    assert t.exec_status == TaskStatus.Errored
    assert t2.exec_status == TaskStatus.Aborted
    assert ("PythonCallable: fn_that_fails -> File('file')"
            in str(excinfo.getrepr()))


@pytest.mark.parametrize('callback', ['on_finish', 'on_render', 'on_failure'])
def test_hook_with_wrong_signature(callback):
    def my_callback(unknown_arg):
        pass

    dag = DAG()
    t = PythonCallable(fn, File('file1.txt'), dag)

    with pytest.raises(CallbackSignatureError):
        setattr(t, callback, my_callback)


def test_task_is_re_executed_if_on_finish_fails(tmp_directory):
    hook_crashing.count = 0
    fn.count = 0

    def make():
        # NOTE: must run callables in the same process so counting works
        dag = DAG(executor=Serial(build_in_subprocess=False))
        t = PythonCallable(fn, File('file1.txt'), dag)
        t.on_finish = hook_crashing
        return dag

    dag = make()

    # first build: fn passes but on_finish crashes
    try:
        dag.build()
    except DAGBuildError:
        pass

    assert hook_crashing.count == 1
    assert fn.count == 1

    # second build: fn should run again, on_finish hook breaks the DAG again
    try:
        dag.build()
    except DAGBuildError:
        pass

    assert hook_crashing.count == 2
    assert fn.count == 2

    # re-instantiating should also run both, since no metadata is saved
    dag_new = make()

    try:
        dag_new.build()
    except DAGBuildError:
        pass

    assert hook_crashing.count == 3
    assert fn.count == 3


def on_finish_sql(task, product, client):
    on_finish_sql.task = task
    on_finish_sql.product = product
    on_finish_sql.client = client


def test_can_request_params(sqlite_client_and_tmp_dir):
    on_finish_sql.task = None
    on_finish_sql.product = None
    on_finish_sql.client = None

    client, _ = sqlite_client_and_tmp_dir
    dag = DAG()

    dag.clients[SQLScript] = client
    dag.clients[SQLiteRelation] = client

    t = SQLScript('CREATE TABLE {{product}} AS SELECT * FROM data',
                  SQLiteRelation(('new_table', 'table')),
                  dag,
                  name='t')

    t.on_finish = on_finish_sql

    dag.build()

    assert on_finish_sql.task is t
    assert on_finish_sql.product is t.product
    assert on_finish_sql.client is client
