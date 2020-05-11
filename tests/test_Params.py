import pytest
from ploomber.tasks.Params import Params


def test_cannot_init_with_upstream_key():
    with pytest.raises(ValueError) as excinfo:
        Params({'upstream': None})

    msg = ('Task params cannot be initialized with an '
           '"upstream" key as it automatically added upon rendering')
    assert str(excinfo.value) == msg


def test_cannot_init_with_product_key():
    with pytest.raises(ValueError) as excinfo:
        Params({'product': None})

    msg = ('Task params cannot be initialized with an '
           '"product" key as it automatically added upon rendering')
    assert str(excinfo.value) == msg


def test_get_param():
    p = Params({'a': 1})
    assert p['a'] == 1


def test_cannot_modify_param():
    p = Params({'a': 1})

    with pytest.raises(RuntimeError):
        p['a'] = 1
