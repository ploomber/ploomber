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


@pytest.mark.parametrize('copy, expected', [[False, True], [True, False]])
def test_init_from_dict(copy, expected):
    d = {'upstream': None, 'product': None}
    params = Params._from_dict(d, copy=copy)
    assert (params._dict is d) is expected


def test_set_item():
    params = Params._from_dict({'a': 1})
    params._setitem('a', 2)
    assert params['a'] == 2
