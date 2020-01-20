import subprocess

from ploomber.dag import DAG
from ploomber.tasks import BashCommand
from ploomber.products import File

import pytest


@pytest.fixture
def dag():
    dag = DAG()

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    t1 = BashCommand('echo a > {{product}} ', File('1.txt'), dag,
                     't1', {}, kwargs, False)

    t2 = BashCommand(('cat {{upstream["t1"]}} > {{product}}'
                      '&& echo b >> {{product}} '),
                     File(('2_{{upstream["t1"]}}')),
                     dag,
                     't2', {}, kwargs, False)

    t3 = BashCommand(('cat {{upstream["t2"]}} > {{product}} '
                      '&& echo c >> {{product}}'),
                     File(('3_{{upstream["t2"]}}')), dag,
                     't3', {}, kwargs, False)

    t1 >> t2 >> t3

    return dag


def can_access_product_without_rendering_if_literal():
    dag = DAG()

    BashCommand('echo a > {{product}}', File('1.txt'), dag,
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

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    ta = BashCommand('touch {{product}}',
                     File('a.txt'), sub_dag, 'ta',
                     {}, kwargs, False)
    tb = BashCommand('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), sub_dag, 'tb',
                     {}, kwargs, False)

    dag = DAG('dag')

    tc = BashCommand('cat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), dag, 'tc',
                     {}, kwargs, False)
    td = BashCommand('cat {{upstream["tc"]}} > {{product}}',
                     File('d.txt'), dag, 'td',
                     {}, kwargs, False)

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
