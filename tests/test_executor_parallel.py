import pytest
from pathlib import Path


from ploomber.exceptions import TaskBuildError
from ploomber import DAG
from ploomber.products import File
from ploomber.tasks import PythonCallable


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


def test_parallel_execution_with_crashing_step(tmp_directory):
    dag = DAG(executor='parallel')
    PythonCallable(failing, File('a_file.txt'), dag, name='t1')

    with pytest.raises(TaskBuildError, match='Exception: Bad things happened'):
        dag.build()


def test_parallel_execution_runs_all_possible_tasks(tmp_directory):
    # import logging
    # logging.basicConfig(level='DEBUG')
    dag = DAG(executor='parallel')
    t_fail = PythonCallable(failing, File('t_fail.txt'), dag, name='t_fail')
    t_fail_downstream = PythonCallable(failing, File('t_fail_downstream.txt'),
                                       dag, name='t_fail_downstream')

    t_fail >> t_fail_downstream

    PythonCallable(touch_root, File('t_ok.txt'), dag, name='t_ok')

    try:
        dag.build(force=True)
    except TaskBuildError:
        pass

    assert not Path('t_fail.txt').exists()
    assert not Path('t_fail_downstream.txt').exists()
    assert Path('t_ok.txt').exists()
