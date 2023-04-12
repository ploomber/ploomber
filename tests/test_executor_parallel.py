from pathlib import Path

from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import PythonCallable

# FIXME: test_dag also uses the parallel executor to test interaction of the
# executor with other features, here, we should add executor-specific features
# to test (i.e. things defined in the executor itself). The current test
# just verifies dag execution finishes


def touch_root(product):
    Path(str(product)).touch()


def touch(upstream, product):
    Path(str(product)).touch()


def failing(product):
    raise Exception("Bad things happened")


def test_parallel_execution(tmp_directory):
    dag = DAG("dag", executor="parallel")

    a1 = PythonCallable(touch_root, File("a1.txt"), dag, "a1")
    a2 = PythonCallable(touch_root, File("a2.txt"), dag, "a2")
    b = PythonCallable(touch, File("b.txt"), dag, "b")
    c = PythonCallable(touch, File("c.txt"), dag, "c")

    (a1 + a2) >> b >> c

    dag.build()
