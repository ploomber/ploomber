import re
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest

from ploomber import DAG
from ploomber.cli.parsers import CustomParser
from ploomber.env.envdict import EnvDict
from ploomber.cli import parsers
from ploomber.env import expand


def test_custom_parser_static_args():

    parser = CustomParser()

    assert set(parser.static_args) == {
        'h', 'help', 'log', 'l', 'entry_point', 'e', 'log_file', 'F'
    }


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


def test_add_static_mutually_exclusive_group(capsys):

    parser = CustomParser()

    with parser:
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--one', '-o', action='store_true')
        group.add_argument('--two', '-t', action='store_true')

    with pytest.raises(SystemExit):
        parser.parse_args(args=['-o', '-t'])

    captured = capsys.readouterr()
    assert 'not allowed with argument' in captured.err


def test_add_dynamic_mutually_exclusive_group(capsys):

    parser = CustomParser()

    with parser:
        pass

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--one', '-o', action='store_true')
    group.add_argument('--two', '-t', action='store_true')

    with pytest.raises(SystemExit):
        parser.parse_args(args=['-o', '-t'])

    captured = capsys.readouterr()
    assert 'not allowed with argument' in captured.err


def test_add_dynamic_arguments():
    parser = CustomParser()

    with parser:
        parser.add_argument('--static-arg', '-s')

    parser.add_argument('--dynamic-arg', '-d')

    added = {'dynamic_arg', 'd'}
    assert not set(parser.static_args) & added


def test_default_loaded_from_env_var(tmp_directory, monkeypatch):
    Path('pipeline.yaml').touch()
    Path('dag.yaml').touch()
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

    dag, args = parser.load_from_entry_point_arg()

    mock.assert_called_once_with('pipeline.yaml')


def test_dagspec_initialization_from_yaml_and_env(tmp_nbs, monkeypatch):
    """
    DAGSpec can be initialized with a path to a spec or a dictionary, but
    they have a slightly different behavior. This ensure the cli passes
    the path, instead of a dictionary
    """
    mock_DAGSpec = Mock(wraps=parsers.DAGSpec)
    mock_default_path_to_env = Mock(
        wraps=parsers.default.path_to_env_from_spec)
    mock_EnvDict = Mock(wraps=parsers.EnvDict)

    monkeypatch.setattr(sys, 'argv', ['python'])
    monkeypatch.setattr(parsers, 'DAGSpec', mock_DAGSpec)
    monkeypatch.setattr(parsers.default, 'path_to_env_from_spec',
                        mock_default_path_to_env)
    monkeypatch.setattr(parsers, 'EnvDict', mock_EnvDict)

    # ensure current timestamp does not change
    mock = Mock()
    mock.datetime.now().isoformat.return_value = 'current-timestamp'
    monkeypatch.setattr(expand, "datetime", mock)

    parser = CustomParser()

    with parser:
        pass

    dag, args = parser.load_from_entry_point_arg()

    # ensure called using the path to the yaml spec
    mock_DAGSpec.assert_called_once_with('pipeline.yaml',
                                         env=EnvDict({'sample': False},
                                                     path_to_here='.'))

    # and EnvDict initialized from env.yaml
    mock_EnvDict.assert_called_once_with(str(Path('env.yaml').resolve()),
                                         path_to_here=Path('.'))


def test_entry_point_from_factory_in_environment_variable(
        backup_test_pkg, monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['python'])
    monkeypatch.setenv('ENTRY_POINT', 'test_pkg.entry.plain_function')
    parser = CustomParser()

    with parser:
        pass

    dag, _ = parser.load_from_entry_point_arg()

    assert isinstance(dag, DAG)


def test_shows_default_value(tmp_directory, capsys):
    Path('pipeline.yaml').touch()
    parser = CustomParser()

    with parser:
        pass

    parser.print_help()

    captured = capsys.readouterr()
    assert 'defaults to pipeline.yaml' in captured.out


def test_shows_default_value_from_env_var(tmp_directory, monkeypatch, capsys):
    monkeypatch.setenv('ENTRY_POINT', 'dag.yaml')

    Path('dag.yaml').touch()
    parser = CustomParser()

    with parser:
        pass

    parser.print_help()

    captured = capsys.readouterr()
    assert re.search(r'defaults\s+to\s+dag.yaml\s+\(ENTRY_POINT\s+env\s+var\)',
                     captured.out)


def test_log(tmp_nbs, monkeypatch):
    mock = Mock()
    monkeypatch.setattr(
        sys, 'argv',
        ['python', '--log', 'info', '--entry-point', 'pipeline.yaml'])
    monkeypatch.setattr(parsers, 'logging', mock)

    parser = CustomParser()

    with parser:
        pass

    parser.load_from_entry_point_arg()

    mock.basicConfig.assert_called_with(level='INFO')


@pytest.mark.parametrize('opt', ['--log-file', '-F'])
def test_log_file(tmp_nbs, monkeypatch, opt):
    mock = Mock()
    monkeypatch.setattr(sys, 'argv', [
        'python', '--log', 'info', opt, 'my.log', '--entry-point',
        'pipeline.yaml'
    ])
    monkeypatch.setattr(parsers, 'logging', mock)

    parser = CustomParser()

    with parser:
        pass

    parser.load_from_entry_point_arg()

    mock.basicConfig.assert_called_with(level='INFO')
    mock.FileHandler.assert_called_with('my.log')
    mock.getLogger().addHandler.assert_called()


@pytest.mark.parametrize('opt', ['--log-file', '-F'])
def test_log_file_with_factory_entry_point(backup_test_pkg, monkeypatch, opt):
    mock = Mock()
    monkeypatch.setattr(sys, 'argv', [
        'python', '--log', 'info', opt, 'my.log', '--entry-point',
        'test_pkg.entry.plain_function'
    ])
    monkeypatch.setattr(parsers, 'logging', mock)

    parser = CustomParser()

    with parser:
        pass

    parser.load_from_entry_point_arg()

    mock.basicConfig.assert_called_with(level='INFO')
    mock.FileHandler.assert_called_with('my.log')
    mock.getLogger().addHandler.assert_called()
