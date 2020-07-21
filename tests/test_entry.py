import pytest

import os
import importlib
import sys
from pathlib import Path

import click
from ploomber.entry import entry
from ploomber import cli


def test_complete_case(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, 'argv',
        ['python', 'test_pkg.entry.with_doc', '--action', 'build'])
    entry._main()


def test_log_enabled(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', 'test_pkg.entry.with_doc', '--action', 'build', '--log',
        'INFO'
    ])
    entry._main()


def test_replace_env_value(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', 'test_pkg.entry.with_doc', '--action', 'build',
        '--env__path__data', '/another/path'
    ])
    entry._main()


def test_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', 'test_pkg.entry.with_param', '--action', 'build',
        'some_value_for_param'
    ])
    entry._main()


def test_no_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, 'argv', ['python', 'test_pkg.entry.no_doc', '--action', 'build'])
    entry._main()


def test_incomplete_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, 'argv',
        ['python', 'test_pkg.entry.incomplete_doc', '--action', 'build'])
    entry._main()


def test_invalid_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, 'argv',
        ['python', 'test_pkg.entry.invalid_doc', '--action', 'build'])
    entry._main()


def test_invalid_module_arg(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['python', 'invalid_module'])

    with pytest.raises(ImportError):
        entry._main()


def test_nonexisting_module(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['python', 'some_module.some_function'])

    with pytest.raises(ImportError):
        entry._main()


def test_invalid_function(monkeypatch):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.invalid_function'])

    with pytest.raises(AttributeError):
        entry._main()


def test_undecorated_function(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, 'argv',
        ['python', 'test_pkg.entry.plain_function', '--action', 'build'])

    entry._main()


def test_undecorated_function_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', 'test_pkg.entry.plain_function_w_param', '--action', 'build',
        'some_value_for_param'
    ])

    entry._main()


def test_parse_doc():
    doc = """
    Some description

    Parameters
    ----------
    param : int
        Description
    """

    expected = {
        'params': {
            'param': {
                'desc': 'Description',
                'type': 'int'
            }
        },
        'summary': ['Some description']
    }

    assert entry._parse_doc(doc) == expected


def test_parse_doc_if_missing_numpydoc(monkeypatch):
    monkeypatch.setattr(importlib, 'import_module', lambda x: None)

    assert entry._parse_doc("""docstring""") == {'params': {}, 'summary': None}


def test_run_spec(monkeypatch, tmp_directory):
    monkeypatch.setattr(click, 'confirm', lambda text: False)
    monkeypatch.setattr(click, 'prompt', lambda text, type: 'my-project')
    cli._new()
    os.chdir('my-project')
    monkeypatch.setattr(sys, 'argv', ['python', 'pipeline.yaml'])
    entry._main()


def test_run_spec_replace_value(monkeypatch, tmp_directory):
    monkeypatch.setattr(click, 'confirm', lambda text: False)
    monkeypatch.setattr(click, 'prompt', lambda text, type: 'my-project')
    cli._new()
    os.chdir('my-project')
    Path('env.yaml').write_text('sample: false')
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'pipeline.yaml', '--env__sample', 'True'])
    entry._main()
