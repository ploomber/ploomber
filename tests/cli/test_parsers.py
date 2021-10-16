from pathlib import Path
import sys
from argparse import ArgumentParser, ArgumentError
from unittest.mock import Mock

import test_pkg
import yaml
import pytest

from ploomber.cli.parsers import (_add_args_from_callable,
                                  _process_file_dir_or_glob, CustomParser,
                                  _add_cli_args_from_env_dict_keys)
from ploomber.env.envdict import EnvDict


def fn(a: int, b: float, c: str, d: bool, e):
    pass


def test_add_type_to_arg_parser():
    parser = ArgumentParser()

    _add_args_from_callable(parser, fn)

    actions = {action.dest: action for action in parser._actions}

    assert actions['a'].type is int
    assert actions['b'].type is float
    assert actions['c'].type is str
    assert actions['d'].type is bool
    assert actions['e'].type is None


def test_error_if_fn_signature_clashes_with_existing_args():
    def my_fn(log=True):
        pass

    parser = CustomParser()

    with parser:
        pass

    with pytest.raises(ValueError) as excinfo:
        _add_args_from_callable(parser, my_fn)

    expected = ("The signature from 'my_fn' conflicts with existing arguments"
                " in the command-line interface, please rename the following "
                "argument: 'log'")
    assert expected == str(excinfo.value)


def test_doesnt_modify_error_if_unknown_reason():
    def my_fn(log=True):
        pass

    parser = CustomParser()
    arg = Mock()
    arg.options_strings = ['a', 'b']
    parser.add_argument = Mock(side_effect=ArgumentError(None, 'message'))

    with parser:
        pass

    with pytest.raises(ArgumentError) as excinfo:
        _add_args_from_callable(parser, my_fn)

    assert 'message' == str(excinfo.value)


@pytest.mark.parametrize('argv, expected', [
    [['ploomber'], 'some_value'],
    [['ploomber', '--env--tag', 'another_value'], 'another_value'],
])
def test_process_file_or_entry_point_param_replace(argv, expected, monkeypatch,
                                                   tmp_directory):
    d = {
        'meta': {
            'extract_product': False,
            'extract_upstream': False
        },
        'tasks': [{
            'source': 'plot.py',
            'params': {
                'some_param': '{{tag}}',
            },
            'product': 'output/plot.ipynb',
            'name': 'plot'
        }]
    }

    Path('plot.py').write_text('# + tags=["parameters"]')

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(d, f)

    with open('env.yaml', 'w') as f:
        yaml.dump({'tag': 'some_value'}, f)

    monkeypatch.setattr(sys, 'argv', argv)
    parser = CustomParser()

    with parser:
        pass

    dag, args = _process_file_dir_or_glob(parser)

    assert dag['plot'].params['some_param'] == expected


@pytest.mark.parametrize('create_env', [False, True])
def test_use_here_placeholder_when_processing_file(create_env, tmp_directory,
                                                   monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['ploomber'])

    if create_env:
        Path('env.yaml').write_text("""
key: value
""")

    Path('script.py').write_text("""
# + tags=["parameters"]
upstream = None
""")

    Path('pipeline.yaml').write_text("""
tasks:
    - source: script.py
      product: "{{here}}/out.ipynb"
""")

    parser = CustomParser()

    with parser:
        pass

    _process_file_dir_or_glob(parser)


@pytest.mark.parametrize('default', [False, True])
def test_cli_from_bool_flag(default, monkeypatch):
    def factory(flag: bool = default):
        pass

    monkeypatch.setattr(sys, 'argv', ['python', '--flag'])
    monkeypatch.setattr(test_pkg, 'mocked_factory', factory, raising=False)

    parser = CustomParser()

    with parser:
        pass

    parser.process_factory_dotted_path('test_pkg.mocked_factory')

    actions = {a.dest: a for a in parser._actions}

    # default should match with function's signature
    assert actions['flag'].default is default
    # passing the flag should flip the value
    assert actions['flag'].const is not default


@pytest.mark.parametrize('default', ['hi', 1, 1.1])
def test_cli_from_param(default, monkeypatch):
    def factory(param=default):
        return param

    monkeypatch.setattr(sys, 'argv', ['python', '--param', 'value'])
    monkeypatch.setattr(test_pkg, 'mocked_factory', factory, raising=False)

    parser = CustomParser()

    with parser:
        pass

    returned, args = parser.process_factory_dotted_path(
        'test_pkg.mocked_factory')
    actions = {a.dest: a for a in parser._actions}

    assert actions['param'].default == default
    assert args.param == 'value'
    assert returned == 'value'


def test_cli_from_param_with_annotation(monkeypatch):
    def factory(param: int = 42):
        return param

    monkeypatch.setattr(sys, 'argv', ['python', '--param', '41'])
    monkeypatch.setattr(test_pkg, 'mocked_factory', factory, raising=False)

    parser = CustomParser()

    with parser:
        pass

    returned, args = parser.process_factory_dotted_path(
        'test_pkg.mocked_factory')
    actions = {a.dest: a for a in parser._actions}

    assert actions['param'].default == 42
    assert args.param == 41
    assert returned == 41


def test_help_does_not_display_location_if_missing_entry_point():
    parser = CustomParser()

    with parser:
        pass

    assert 'Entry point\n' in parser.format_help()


def test_help_displays_location_of_located_entry_point(tmp_directory):
    Path('pipeline.yaml').touch()

    parser = CustomParser()

    with parser:
        pass

    assert 'Entry point, defaults to pipeline.yaml\n' in parser.format_help()


def test_custom_parser_error_if_unable_to_automatically_locate_entry_point(
        capsys):
    parser = CustomParser()

    with parser:
        pass

    with pytest.raises(SystemExit) as excinfo:
        parser.parse_entry_point_value()

    captured = capsys.readouterr()

    assert excinfo.value.code == 2
    assert 'Unable to find a pipeline entry point' in captured.err


def test_error_if_missing_entry_point_value(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['ploomber', '--entry-point'])

    parser = CustomParser()

    with parser:
        pass

    with pytest.raises(SystemExit) as excinfo:
        parser.parse_entry_point_value()

    captured = capsys.readouterr()

    assert excinfo.value.code == 2
    assert ('ploomber: error: argument --entry-point/-e: expected one argument'
            in captured.err)


def test_add_cli_args_from_env_dict_keys():
    parser = ArgumentParser()

    _add_cli_args_from_env_dict_keys(parser, EnvDict({'a': 1}))

    assert {action.dest for action in parser._actions} == {'env__a', 'help'}
