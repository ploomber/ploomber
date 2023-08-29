import pytest

from ploomber_cli.cli import _suggest_command


@pytest.mark.parametrize(
    "name, expected",
    [
        [None, None],
        ["d", "do"],
        ["ake", "make"],
        ["MAKE", "make"],
        ["do", None],
        ["make", None],
        ["run", "build"],
        ["execute", "build"],
    ],
)
def test_suggest_command(name, expected):
    assert _suggest_command(name, ["do", "make"]) == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ["gt-key", "get-key"],
        ["gt", None],
    ],
)
def test_nested_suggest_command(name, expected):
    assert _suggest_command(name, ["set-key", "get-key", "get-pipelines"]) == expected
