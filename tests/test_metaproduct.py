from pathlib import Path

from ploomber import DAG
from ploomber.tasks import ShellScript
from ploomber.products import File, MetaProduct, Product


def test_interface():
    """
    Look for unnecessary implemeneted methods/attributes in MetaProduct,
    this helps us keep the API up-to-date if the Product interface changes
    """

    # look for extra attrs, but allow the ones we get from
    # collections.abc.Mapping
    extra_attrs = {
        attr
        for attr in set(dir(MetaProduct)) - set(dir(Product))
        if not attr.startswith('__')
    } - {'get', 'keys', 'items', 'values'}

    if extra_attrs:
        raise ValueError(
            'The following methods/attributes in MetaProduc '
            'are not part of the Product interface: {}'.format(extra_attrs))


def test_get():
    a = File('1.txt')
    m = MetaProduct({'a': a})

    assert m.get('a') is a
    assert m.get('b') is None


def test_delete_metadata(tmp_directory):
    Path('a.txt.source').touch()
    Path('b.txt.source').touch()

    a = File('a.txt')
    b = File('b.txt')
    m = MetaProduct({'a': a, 'b': b})
    m.metadata.delete()

    assert not Path('a.txt.source').exists()
    assert not Path('b.txt.source').exists()


def test_can_iterate_over_products():
    p1 = File('1.txt')
    p2 = File('2.txt')
    m = MetaProduct([p1, p2])

    assert set(m) == {p1, p2}


def test_can_iterate_when_initialized_with_dictionary():
    p1 = File('1.txt')
    p2 = File('2.txt')
    m = MetaProduct({'a': p1, 'b': p2})

    assert set(m) == {p1, p2}


def test_can_create_task_with_more_than_one_product(tmp_directory):
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = ShellScript('touch {{product[0]}} {{product[1]}}',
                     (File(fa), File(fb)), dag, 'ta')
    tc = ShellScript(
        'cat {{upstream["ta"][0]}} {{upstream["ta"][1]}} > '
        '{{product}}', File(fc), dag, 'tc')

    ta >> tc

    dag.render()

    dag.build()
