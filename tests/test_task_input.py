from pathlib import Path

from ploomber import DAG
from ploomber.tasks import Input
from ploomber.products import File


def touch(product, upstream):
    Path(str(product)).touch()


def test_input_always_executes(tmp_directory):
    dag = DAG()

    Path('some_file').touch()
    t1 = Input(File('some_file'), dag, name='some_file')

    assert t1.should_execute()

    dag.build()

    assert t1.should_execute()
