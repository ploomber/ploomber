import pytest

from ploomber.io import pretty_print


@pytest.mark.parametrize(
    "obj, expected",
    [
        [["a"], "'a'"],
        [["a", "b", "c"], "'a', 'b', and 'c'"],
    ],
)
@pytest.mark.parametrize("factory", [set, list, tuple])
def test_iterable(obj, expected, factory):
    assert pretty_print.iterable(factory(obj)) == expected
