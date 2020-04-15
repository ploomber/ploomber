from ploomber.dag import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.exceptions import DAGBuildError


def fn_that_fails(product):
    raise Exception


def hook(dag):
    if hasattr(hook, 'count'):
        hook.count += 1


def test_on_finish():
    hook.count = 0

    dag = DAG()
    dag.on_finish = hook
    dag.build()

    assert hook.count == 1


def test_on_failure():
    hook.count = 0

    dag = DAG()

    PythonCallable(fn_that_fails, File('some_file.txt'), dag)

    dag.on_failure = hook

    try:
        dag.build()
    except DAGBuildError:
        pass

    assert hook.count == 1
