import pytest

from ploomber.cli.cli import _suggest_command


@pytest.mark.parametrize('name, expected', [
    [None, None],
    ['d', 'do'],
    ['ake', 'make'],
    ['MAKE', 'make'],
    ['do', None],
    ['make', None],
    ['run', 'build'],
    ['execute', 'build'],
])
def test_suggest_command(name, expected):
    assert _suggest_command(name, ['do', 'make']) == expected
