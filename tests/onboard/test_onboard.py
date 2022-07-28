import subprocess
from unittest.mock import Mock, MagicMock
import click
import pytest
from pathlib import Path
from typing import Callable

from ploomber.onboard import __main__ as onboard

from ploomber.cli import examples
from ploomber import DAG
import ploomber

def test_make_open_callable():
    callable_open = onboard._make_open_callable('open')
    callable_xdg = onboard._make_open_callable('xdg-open')

    assert isinstance(callable_open, Callable)
    assert isinstance(callable_xdg, Callable)


def test_echo_with_confirm(monkeypatch, capsys):
    magic_mock = MagicMock()
    monkeypatch.setattr(click, 'prompt', magic_mock)
    onboard._echo('mock message', confirm=True)
    captured = capsys.readouterr()

    assert 'continue' not in captured.out
    assert 'mock message' in captured.out


def test_echo_without_confirm(capsys):
    onboard._echo('mock message', confirm=False)
    captured = capsys.readouterr()

    assert '\nmock message' in captured.out
    assert 'continue' not in captured.out


def test_try_open_success(monkeypatch, tmp_directory, capsys):
    mock = Mock()
    monkeypatch.setattr(subprocess, 'run', mock)
    Path('pipeline.yaml').write_text('test')
    onboard._try_open('pipeline.yaml')
    captured = capsys.readouterr()

    assert 'Opening' in captured.out


def test_try_open_with_message(monkeypatch, tmp_directory, capsys):
    mock = Mock()
    monkeypatch.setattr(subprocess, 'run', mock)
    path = Path('pipeline.yaml')
    onboard._try_open_with_message(path)
    captured = capsys.readouterr()

    assert 'Done' in captured.out
    assert 'Opening' in captured.out


def test_load_dag(tmp_nbs):
    dag = onboard._load_dag(tmp_nbs)

    assert 'load.py' in str(dag['load'].source.loc)
    assert isinstance(dag, DAG)


def test_cmd(capsys):
    onboard._cmd('open')
    captured = capsys.readouterr()

    assert 'Running' in captured.out


@pytest.fixture(scope='function')
def clone_examples():
    examples.main(name=None, force=True)


def test_main(clone_examples, monkeypatch, capsys, tmp_directory):
    magic_mock = MagicMock()
    magic_mock_return = MagicMock(return_value=True)
    monkeypatch.setattr(subprocess, 'run', magic_mock)
    monkeypatch.setattr(click, 'prompt', magic_mock)
    monkeypatch.setattr(onboard, '_load_dag', magic_mock)
    monkeypatch.setattr(examples._ExamplesManager, 'download', magic_mock)
    monkeypatch.setattr(Path, 'relative_to', magic_mock)
    monkeypatch.setattr(onboard, '_modified_task', magic_mock_return)
    monkeypatch.setattr(onboard, '_try_open', magic_mock)
    monkeypatch.setattr(ploomber.cli.cloud, '_email_input', magic_mock)
    monkeypatch.setattr('sys.stdin', 'mock_input')

    onboard.main()
    captured = capsys.readouterr()

    assert "Ploomber allows you to write modular data pipeline" in captured.out
    assert "Let's install the dependencies!" in captured.out
    assert "Let's plot the pipeline" in captured.out
    assert "You can execute the pipeline with one command" in captured.out
    assert "Ploomber automatically generates reports" in captured.out
    assert "Please join our community" in captured.out
