from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.exceptions import DAGBuildError


def fn_that_fails(product):
    raise Exception


def on_finish_hook(dag, report):
    if hasattr(on_finish_hook, 'count'):
        on_finish_hook.count += 1

    on_finish_hook.report = report


def on_failure_hook(dag, traceback):
    if hasattr(on_failure_hook, 'count'):
        on_failure_hook.count += 1

    on_failure_hook.traceback = traceback


def test_on_finish():
    on_finish_hook.count = 0
    on_finish_hook.report = None

    dag = DAG()
    dag.on_finish = on_finish_hook
    report = dag.build()

    assert on_finish_hook.count == 1
    assert report is on_finish_hook.report


def test_on_failure():
    on_failure_hook.count = 0
    on_failure_hook.traceback = None

    dag = DAG()

    PythonCallable(fn_that_fails, File('some_file.txt'), dag)

    dag.on_failure = on_failure_hook

    try:
        dag.build()
    except DAGBuildError:
        pass

    assert on_failure_hook.count == 1
    assert 'Traceback (most recent call last):' in on_failure_hook.traceback
