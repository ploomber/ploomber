import shutil
import warnings
from pathlib import Path

from unittest.mock import Mock
import pytest
import pandas as pd

from ploomber.exceptions import DAGRenderError
from ploomber.constants import TaskStatus
from ploomber import DAG
from ploomber.tasks import ShellScript, PythonCallable, SQLScript
from ploomber.products import File, SQLiteRelation
from ploomber.executors import Serial
from ploomber.clients.storage.local import LocalStorageClient
from ploomber.clients import SQLAlchemyClient


class WarningA(Warning):
    pass


class WarningB(Warning):
    pass


def touch_root(product):
    Path(str(product)).touch()


def touch_root_with_metaproduct(product):
    for p in product:
        Path(str(p)).touch()


def touch_with_metaproduct(upstream, product):
    for p in product:
        Path(str(p)).touch()


def touch(upstream, product):
    Path(str(product)).touch()


def on_render_1():
    warnings.warn('This is a warning', WarningA)


def on_render_2():
    warnings.warn('This is another warning', WarningA)


def on_render_failed():
    raise ValueError


@pytest.fixture
def dag():
    dag = DAG()

    t1 = ShellScript('echo a > {{product}} ', File('1.txt'), dag, 't1')

    t2 = ShellScript(('cat {{upstream["t1"]}} > {{product}}'
                      '&& echo b >> {{product}} '),
                     File(('2_{{upstream["t1"]}}')), dag, 't2')

    t3 = ShellScript(('cat {{upstream["t2"]}} > {{product}} '
                      '&& echo c >> {{product}}'),
                     File(('3_{{upstream["t2"]}}')), dag, 't3')

    t1 >> t2 >> t3

    return dag


def test_dag_render_step_by_step():
    dag = DAG()

    t1 = PythonCallable(touch_root, File('t1.txt'), dag, name='t1')
    t21 = PythonCallable(touch, File('t21.txt'), dag, name='t21')
    t22 = PythonCallable(touch, File('t22.txt'), dag, name='t22')
    t3 = PythonCallable(touch, File('t3.txt'), dag, name='t3')

    t1 >> t21
    t1 >> t22

    (t21 + t22) >> t3

    assert (set(t.exec_status
                for t in dag.values()) == {TaskStatus.WaitingRender})

    t1.render()

    assert t1.exec_status == TaskStatus.WaitingExecution
    assert t21.exec_status == TaskStatus.WaitingRender
    assert t22.exec_status == TaskStatus.WaitingRender
    assert t3.exec_status == TaskStatus.WaitingRender

    t21.render()

    assert t1.exec_status == TaskStatus.WaitingExecution
    assert t21.exec_status == TaskStatus.WaitingUpstream
    assert t22.exec_status == TaskStatus.WaitingRender
    assert t3.exec_status == TaskStatus.WaitingRender

    t22.render()

    assert t1.exec_status == TaskStatus.WaitingExecution
    assert t21.exec_status == TaskStatus.WaitingUpstream
    assert t22.exec_status == TaskStatus.WaitingUpstream
    assert t3.exec_status == TaskStatus.WaitingRender

    t3.render()

    assert t1.exec_status == TaskStatus.WaitingExecution
    assert t21.exec_status == TaskStatus.WaitingUpstream
    assert t22.exec_status == TaskStatus.WaitingUpstream
    assert t3.exec_status == TaskStatus.WaitingUpstream


def test_dag_render_step_by_step_w_skipped(tmp_directory):
    dag = DAG()

    t1 = PythonCallable(touch_root, File('t1.txt'), dag, name='t1')
    t21 = PythonCallable(touch, File('t21.txt'), dag, name='t21')
    t22 = PythonCallable(touch, File('t22.txt'), dag, name='t22')
    t3 = PythonCallable(touch, File('t3.txt'), dag, name='t3')

    t1 >> t21
    t1 >> t22

    (t21 + t22) >> t3

    assert (set(t.exec_status
                for t in dag.values()) == {TaskStatus.WaitingRender})

    dag.render()
    t1.build()

    dag.render()

    assert t1.exec_status == TaskStatus.Skipped
    assert t21.exec_status == TaskStatus.WaitingExecution
    assert t22.exec_status == TaskStatus.WaitingExecution
    assert t3.exec_status == TaskStatus.WaitingUpstream

    t21.build()
    dag.render()

    assert t1.exec_status == TaskStatus.Skipped
    assert t21.exec_status == TaskStatus.Skipped
    assert t22.exec_status == TaskStatus.WaitingExecution
    assert t3.exec_status == TaskStatus.WaitingUpstream

    t22.build()
    dag.render()

    assert t1.exec_status == TaskStatus.Skipped
    assert t21.exec_status == TaskStatus.Skipped
    assert t22.exec_status == TaskStatus.Skipped
    assert t3.exec_status == TaskStatus.WaitingExecution

    t3.build()
    dag.render()

    assert t1.exec_status == TaskStatus.Skipped
    assert t21.exec_status == TaskStatus.Skipped
    assert t22.exec_status == TaskStatus.Skipped
    assert t3.exec_status == TaskStatus.Skipped


def test_can_access_product_without_rendering_if_literal():
    dag = DAG()

    ShellScript('echo a > {{product}}', File('1.txt'), dag, 't1')

    # no rendering!

    # check str works even though we did not run dag.render()
    assert str(dag['t1'].product) == '1.txt'


def test_can_render_templates_in_products(dag, tmp_directory):

    t2 = dag['t2']
    t3 = dag['t3']

    dag.render()

    assert str(t3.product) == '3_2_1.txt'
    assert str(t2.product) == '2_1.txt'


def test_can_render_with_postgres_products(dag, tmp_directory):
    pass


def test_can_render_templates_in_code(dag, tmp_directory):
    pass


def test_can_build_dag_with_templates(dag, tmp_directory):
    pass


def test_rendering_dag_also_renders_upstream_outside_dag(tmp_directory):
    sub_dag = DAG('sub_dag')

    ta = PythonCallable(touch_root, File('a.txt'), sub_dag, 'ta')
    tb = PythonCallable(touch, File('b.txt'), sub_dag, 'tb')

    dag = DAG('dag')

    tc = PythonCallable(touch, File('c.txt'), dag, 'tc')
    td = PythonCallable(touch, File('d.txt'), dag, 'td')

    ta >> tb >> tc >> td

    # FIXME: calling dag.build() alone does not work since .build
    # will be called on tb, tc and td only (not in ta), this is a dag
    # execution problem, when building a dag, if the current task to
    # build is not in the current dag, then its task.build() should build up
    # until that task, instead of just building that task
    # dag.build()

    # this works
    sub_dag.build()
    dag.build()


def test_warnings_are_shown(tmp_directory):
    dag = DAG()
    t1 = PythonCallable(touch_root, File('file.txt'), dag)
    t2 = PythonCallable(touch, File('file2.txt'), dag)
    t1.on_render = on_render_1
    t2.on_render = on_render_2
    t1 >> t2

    with pytest.warns(None) as record:
        dag.render()

    assert len(record) == 1
    assert 'This is a warning' in str(record[0].message)
    assert 'This is another warning' in str(record[0].message)


def test_recover_from_failed_render():
    dag = DAG()
    t1 = PythonCallable(touch_root, File('file.txt'), dag)
    t2 = PythonCallable(touch, File('file2.txt'), dag)
    t1.on_render = on_render_failed
    t2.on_render = on_render_2
    t1 >> t2

    with pytest.raises(DAGRenderError):
        dag.render()

    assert t1.exec_status == TaskStatus.ErroredRender
    assert t2.exec_status == TaskStatus.AbortedRender

    t1.on_render = on_render_1

    dag.render()

    assert t1.exec_status == TaskStatus.WaitingExecution
    assert t2.exec_status == TaskStatus.WaitingUpstream


def test_render_checks_outdated_status_once(monkeypatch, tmp_directory):
    """
    _check_is_outdated is an expensive operation and it should only run
    once per task
    """
    def _make_dag():
        dag = DAG(executor=Serial(build_in_subprocess=False))
        t1 = PythonCallable(touch_root, File('one.txt'), dag, name='one')
        t2 = PythonCallable(touch, File('two.txt'), dag, name='two')
        t1 >> t2
        return dag

    _make_dag().build()

    dag = _make_dag()

    t1 = dag['one']
    t2 = dag['two']

    monkeypatch.setattr(t1.product, '_check_is_outdated',
                        Mock(wraps=t1.product._check_is_outdated))
    monkeypatch.setattr(t2.product, '_check_is_outdated',
                        Mock(wraps=t2.product._check_is_outdated))

    # after building for the first time
    dag.render()

    t1.product._check_is_outdated.assert_called_once()
    t2.product._check_is_outdated.assert_called_once()


def make_dag_with_client():
    dag = DAG(executor=Serial(build_in_subprocess=False))

    dag.clients[File] = LocalStorageClient('remote', path_to_project_root='.')

    root = PythonCallable(touch_root, File('out/root'), dag=dag, name='root')
    task = PythonCallable(touch, File('out/file'), dag=dag, name='task')
    root >> task
    return dag


def make_larger_dag_with_client():
    dag = DAG(executor=Serial(build_in_subprocess=False))

    dag.clients[File] = LocalStorageClient('remote', path_to_project_root='.')

    root = PythonCallable(touch_root, File('out/root'), dag=dag, name='root')
    task = PythonCallable(touch, File('out/file'), dag=dag, name='task')
    another = PythonCallable(touch,
                             File('out/another'),
                             dag=dag,
                             name='another')
    root >> task >> another
    return dag


def make_dag_with_client_and_metaproduct():
    dag = DAG(executor=Serial(build_in_subprocess=False))

    dag.clients[File] = LocalStorageClient('remote', path_to_project_root='.')

    root = PythonCallable(touch_root_with_metaproduct, {
        'root': File('out/root'),
        'another': File('out/another')
    },
                          dag=dag,
                          name='root')
    task = PythonCallable(touch, File('file'), dag=dag, name='task')
    root >> task
    return dag


def make_dag_with_client_and_metaproduct_2():
    dag = DAG(executor=Serial(build_in_subprocess=False))

    dag.clients[File] = LocalStorageClient('remote', path_to_project_root='.')

    root = PythonCallable(touch_root_with_metaproduct, {
        'root': File('out/root'),
        'another': File('out/another')
    },
                          dag=dag,
                          name='root')
    task = PythonCallable(touch_with_metaproduct, {
        'file': File('out/file'),
        'another_file': File('out/another_file')
    },
                          dag=dag,
                          name='task')
    last = PythonCallable(touch, File('last'), dag=dag, name='last')
    root >> task >> last
    return dag


@pytest.mark.parametrize('factory', [
    make_dag_with_client,
    make_dag_with_client_and_metaproduct,
    make_dag_with_client_and_metaproduct_2,
    make_larger_dag_with_client,
])
def test_render_remote(factory, tmp_directory):
    factory().build()
    shutil.rmtree('out')

    dag = factory()
    dag.render(remote=True)

    assert set(t.exec_status for t in dag.values()) == {TaskStatus.Skipped}


def test_render_remote_checks_remote_timestamp(tmp_directory):
    make_dag_with_client().build()

    Path('remote', 'out', 'root').unlink()

    dag = make_dag_with_client()
    dag.render(remote=True)

    status = {n: t.exec_status for n, t in dag.items()}

    assert status['root'] == TaskStatus.WaitingExecution
    assert status['task'] == TaskStatus.WaitingUpstream


def test_render_remote_with_non_file_products(tmp_directory):
    client = SQLAlchemyClient('sqlite:///my.db')

    pd.DataFrame({'x': range(3)}).to_sql('data', client.engine)

    def make(client):
        dag = DAG()
        dag.clients[SQLScript] = client
        dag.clients[SQLiteRelation] = client

        SQLScript('CREATE TABLE {{product}} AS SELECT * FROM data',
                  SQLiteRelation(['data2', 'table']),
                  dag=dag,
                  name='task')

        return dag

    make(client).build()

    dag = make(client)
    dag.render(remote=True)
    client.close()

    # should match the local status
    assert {t.exec_status for t in dag.values()} == {TaskStatus.Skipped}
