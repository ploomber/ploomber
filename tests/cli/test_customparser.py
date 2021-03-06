import sys
import pytest
from ploomber.cli.parsers import CustomParser


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
