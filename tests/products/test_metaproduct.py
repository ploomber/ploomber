from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File, MetaProduct


def touch_all(product):
    for p in product:
        Path(str(p)).touch()


def touch_all_upstream(product, upstream):
    for p in product:
        Path(str(p)).touch()


def test_get():
    a = File('1.txt')
    m = MetaProduct({'a': a})

    assert m.get('a') is a
    assert m.get('b') is None


def test_delete_metadata(tmp_directory):
    Path('.a.txt.metadata').touch()
    Path('.b.txt.metadata').touch()

    a = File('a.txt')
    b = File('b.txt')
    m = MetaProduct({'a': a, 'b': b})
    m.metadata.delete()

    assert not Path('.a.txt.metadata').exists()
    assert not Path('.b.txt.metadata').exists()


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
    fd = Path('d.txt')

    ta = PythonCallable(touch_all, (File(fa), File(fb)), dag, 'ta')
    tc = PythonCallable(touch_all_upstream, (File(fc), File(fd)), dag, 'tc')

    ta >> tc

    dag.build()

    assert fa.exists()
    assert fb.exists()
    assert fc.exists()
    assert fd.exists()


def test_download():
    p1, p2 = Mock(), Mock()
    m = MetaProduct({'a': p1, 'b': p2})

    m.download()

    p1.download.assert_called_once_with()
    p2.download.assert_called_once_with()


def test_upload():
    p1, p2 = Mock(), Mock()
    m = MetaProduct({'a': p1, 'b': p2})

    m.upload()

    p1.upload.assert_called_once_with()
    p2.upload.assert_called_once_with()


def test_error_if_metaproduct_initialized_with_non_products():
    def do_stuff():
        pass

    with pytest.raises(AttributeError) as excinfo:
        PythonCallable(do_stuff, {'a': 'not-a-product'}, dag=DAG())

    assert 'Expected MetaProduct to initialize' in str(excinfo.value)
    assert "'not-a-product'" in str(excinfo.value)
    assert "<class 'str'>" in str(excinfo.value)
    assert "File('not-a-product')" in str(excinfo.value)


@pytest.mark.parametrize(
    'arg, expected',
    [
        [
            {
                'a': 1,
                'b': 2
            },
            "MetaProduct({'a': 1, 'b': 2})",
        ],
        [
            {
                'a': 1,
                'b': 2,
                'c': 3,
                'd': 4,
                'e': 5
            },
            "MetaProduct({'a': 1, 'b': 2, 'c': 3, 'd': 4, ...})",
        ],
    ],
)
def test_repr(arg, expected):
    assert repr(MetaProduct(arg)) == expected
