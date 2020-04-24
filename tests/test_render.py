import warnings
from pathlib import Path

import pytest

from ploomber.constants import TaskStatus
from ploomber import DAG
from ploomber.tasks import ShellScript, PythonCallable
from ploomber.products import File


class WarningA(Warning):
    pass


class WarningB(Warning):
    pass


def touch_root(product):
    Path(str(product)).touch()


def touch(upstream, product):
    Path(str(product)).touch()


def on_render_1():
    warnings.warn('This is a warning', WarningA)


def on_render_2():
    warnings.warn('This is another warning', WarningA)


@pytest.fixture
def dag():
    dag = DAG()

    t1 = ShellScript('echo a > {{product}} ', File('1.txt'), dag,
                     't1')

    t2 = ShellScript(('cat {{upstream["t1"]}} > {{product}}'
                      '&& echo b >> {{product}} '),
                     File(('2_{{upstream["t1"]}}')),
                     dag,
                     't2')

    t3 = ShellScript(('cat {{upstream["t2"]}} > {{product}} '
                      '&& echo c >> {{product}}'),
                     File(('3_{{upstream["t2"]}}')), dag,
                     't3')

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

    assert (set(t.exec_status for t in dag.values())
            == {TaskStatus.WaitingRender})

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

    assert (set(t.exec_status for t in dag.values())
            == {TaskStatus.WaitingRender})

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

    ShellScript('echo a > {{product}}', File('1.txt'), dag,
                't1')

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

    ta = ShellScript('touch {{product}}',
                     File('a.txt'), sub_dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), sub_dag, 'tb')

    dag = DAG('dag')

    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), dag, 'tc')
    td = ShellScript('cat {{upstream["tc"]}} > {{product}}',
                     File('d.txt'), dag, 'td')

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
    # assert isinstance(record[0], WarningA)
    # assert isinstance(record[1], WarningB)
