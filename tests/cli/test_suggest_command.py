import pytest

from ploomber_cli.cli import _suggest_command, cmd_router
import sys


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


@pytest.mark.parametrize('name, expected', [
    ['gt-key', 'get-key'],
    ['gt', None],
])
def test_nested_suggest_command(name, expected):
    assert _suggest_command(
        name, ['set-key', 'get-key', 'get-pipelines']) == expected


@pytest.mark.parametrize('cmd, nested_cmd, suggestion', [
    ['cloud', 'gt-key', 'get-key'],
    ['cloud', 'gt', None],
])
def test_nested_suggestions(monkeypatch, capsys, cmd, nested_cmd, suggestion):
    monkeypatch.setattr(sys, 'argv', ['ploomber', cmd, nested_cmd])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()

    if suggestion:
        assert f"Did you mean '{cmd} {suggestion}'?" in captured.err
    else:
        assert f"No such command '{nested_cmd}'" in captured.err
    assert excinfo.value.code == 2
