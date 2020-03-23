from pathlib import Path

from ploomber import DAG
from ploomber.tasks import Link, PythonCallable
from ploomber.products import File


def touch(product, upstream):
    Path(str(product)).touch()


def test_link_is_up_to_date(tmp_directory):
    dag = DAG()

    t1 = Link(File('some_file'), dag, name='some_file')

    assert not t1.should_execute()


def test_downstream_from_link_is_up_to_date_after_build(tmp_directory):
    dag = DAG()

    Path('some_file').touch()

    t1 = Link(File('some_file'), dag, name='some_file')
    t2 = PythonCallable(touch, File('another_file'), dag)
    t1 >> t2

    dag.build()

    assert not t2.should_execute()
