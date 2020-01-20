import subprocess
from pathlib import Path

from ploomber.dag import DAG
from ploomber.tasks import BashCommand
from ploomber.products import File, MetaProduct


def test_can_iterate_over_products():
    p1 = File('1.txt')
    p2 = File('2.txt')
    m = MetaProduct([p1, p2])

    assert list(m) == [p1, p2]


def test_can_iterate_when_initialized_with_dictionary():
    p1 = File('1.txt')
    p2 = File('2.txt')
    m = MetaProduct({'a': p1, 'b': p2})

    assert list(m) == [p1, p2]


def test_can_create_task_with_more_than_one_product(tmp_directory):
    dag = DAG()

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch {{product[0]}} {{product[1]}}',
                     (File(fa), File(fb)), dag, 'ta',
                     {}, kwargs, False)
    tc = BashCommand('cat {{upstream["ta"][0]}} {{upstream["ta"][1]}} > {{product}}',
                     File(fc), dag, 'tc',
                     {}, kwargs, False)

    ta >> tc

    dag.render()

    dag.build()
