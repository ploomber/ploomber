import pytest

from ploomber.spec.dagspec import _expand_upstream


@pytest.fixture
def task_names():
    return ['one', 'another', 'prefix-1', 'prefix-2']


@pytest.mark.parametrize('upstream, expected', [
    [None, None],
    [{'one'}, {'one'}],
    [['prefix-*'], {'prefix-1', 'prefix-2'}],
    [['one', 'prefix-*'], {'one', 'prefix-1', 'prefix-2'}],
])
def test_expand_upstream(upstream, task_names, expected):
    assert _expand_upstream(upstream, task_names) == expected
