import pytest
from ploomber.tasks.Params import Params


def test_get_param():
    p = Params({'a': 1})
    assert p['a'] == 1


def test_cannot_modify_param():
    p = Params({'a': 1})

    with pytest.raises(RuntimeError):
        p['a'] = 1
