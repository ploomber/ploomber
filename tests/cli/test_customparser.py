from pathlib import Path
import sys
from unittest.mock import Mock

import pytest

from ploomber.cli.parsers import CustomParser, _custom_command
from ploomber.cli import parsers


def test_custom_parser_static_args():

    parser = CustomParser()

    assert set(
        parser.static_args) == {'h', 'help', 'log', 'l', 'entry_point', 'e'}


def test_cannot_add_arguments_without_context_manager():
    parser = CustomParser()

    with pytest.raises(RuntimeError):
        parser.add_argument('--static-arg', '-s')


def test_add_static_arguments():
    parser = CustomParser()

    with parser:
        parser.add_argument('--static-arg', '-s')

    added = {'static_arg', 's'}
    assert set(parser.static_args) & added == added


def test_add_dynamic_arguments():
    parser = CustomParser()

    with parser:
        parser.add_argument('--static-arg', '-s')

    parser.add_argument('--dynamic-arg', '-d')

    added = {'dynamic_arg', 'd'}
    assert not set(parser.static_args) & added


def test_default_loaded_from_env_var(monkeypatch):
    monkeypatch.setenv('ENTRY_POINT', 'dag.yaml')
    monkeypatch.setattr(sys, 'argv', ['ploomber'])

    parser = CustomParser()

    assert parser.DEFAULT_ENTRY_POINT == 'dag.yaml'

    args = parser.parse_args()
    assert args.entry_point == 'dag.yaml'


def test_dagspec_initialization_from_yaml(tmp_nbs_nested, monkeypatch):
    """
    DAGSpec can be initialized with a path to a spec or a dictionary, but
    they have a slightly different behavior. This checks that we initialize
    with the path
    """
    mock = Mock(wraps=parsers.DAGSpec)

    monkeypatch.setattr(sys, 'argv', ['python'])
    monkeypatch.setattr(parsers, 'DAGSpec', mock)

    parser = CustomParser()

    with parser:
        pass

    dag, args = _custom_command(parser)

    mock.assert_called_once_with('pipeline.yaml')


def test_dagspec_initialization_from_yaml_and_env(tmp_nbs, monkeypatch):
    """
    DAGSpec can be initialized with a path to a spec or a dictionary, but
    they have a slightly different behavior. This checks that we initialize
    with the path
    """
    mock_dagspec = Mock(wraps=parsers.DAGSpec)
    mock_default_path_to_env = Mock(wraps=parsers.default.path_to_env)
    mock_envdict = Mock(wraps=parsers.EnvDict)

    monkeypatch.setattr(sys, 'argv', ['python'])
    monkeypatch.setattr(parsers, 'DAGSpec', mock_dagspec)
    monkeypatch.setattr(parsers.default, 'path_to_env',
                        mock_default_path_to_env)
    monkeypatch.setattr(parsers, 'EnvDict', mock_envdict)

    parser = CustomParser()

    with parser:
        pass

    dag, args = _custom_command(parser)

    mock_dagspec.assert_called_once_with('pipeline.yaml',
                                         env={'sample': False})
    mock_default_path_to_env.assert_called_once_with(Path('.'))
    mock_envdict.assert_called_once_with(str(Path('env.yaml').resolve()))
