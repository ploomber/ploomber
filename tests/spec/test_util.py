import pytest

from ploomber.spec.dagspec import _expand_upstream


@pytest.fixture
def task_names():
    return ["one", "another", "prefix-2", "prefix-1"]


@pytest.mark.parametrize(
    "upstream, expected",
    [
        [None, None],
        [{"one"}, {"one": None}],
        [["prefix-*"], {"prefix-1": "prefix-*", "prefix-2": "prefix-*"}],
        [
            ["prefix-*", "one"],
            {"one": None, "prefix-1": "prefix-*", "prefix-2": "prefix-*"},
        ],
    ],
)
def test_expand_upstream(upstream, task_names, expected):
    assert _expand_upstream(upstream, task_names) == expected
