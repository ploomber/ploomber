from pathlib import Path


from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import PythonCallable

# executors are responsible for reporting Executed and Errored status
# should check that they do it


def touch_root(product):
    Path(str(product)).touch()


def touch(upstream, product):
    Path(str(product)).touch()


def failing(product):
    raise Exception('Bad things happened')


def test_parallel_execution(tmp_directory):
    dag = DAG('dag', executor='parallel')

    a1 = PythonCallable(touch_root, File('a1.txt'), dag, 'a1')
    a2 = PythonCallable(touch_root, File('a2.txt'), dag, 'a2')
    b = PythonCallable(touch, File('b.txt'), dag, 'b')
    c = PythonCallable(touch, File('c.txt'), dag, 'c')

    (a1 + a2) >> b >> c

    dag.build()
