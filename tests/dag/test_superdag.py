from pathlib import Path

from ploomber import DAG
from ploomber.dag.superdag import SuperDAG
from ploomber.tasks import PythonCallable
from ploomber.products import File

# FIXME: bug when rendering dag2.render() when names are 1 and 2 for both dags


def touch_root(product):
    Path(str(product)).touch()


def touch(upstream, product):
    Path(str(product)).touch()


def test_superdag(tmp_directory):
    dag1 = DAG()

    t11 = PythonCallable(touch_root, File("11.txt"), dag1, name="11")
    t12 = PythonCallable(touch, File("12.txt"), dag1, name="12")

    dag2 = DAG()

    t21 = PythonCallable(touch, File("21.txt"), dag2, name="21")
    t22 = PythonCallable(touch, File("22.txt"), dag2, name="22")

    t11 >> t12 >> t21 >> t22

    dag = SuperDAG([dag1, dag2])

    dag.render()

    dag.build()

    files = ["11.txt", "12.txt", "21.txt", "22.txt"]

    assert set(dag.G.nodes()) == {t11, t12, t21, t22}
    assert all(Path(name).exists() for name in files)
