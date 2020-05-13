import pytest

import importlib
import sys

from ploomber import entry


def test_complete_case(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.with_doc', 'build'])
    entry.main()


def test_log_enabled(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.with_doc', 'build',
                         '--log', 'INFO'])
    entry.main()


def test_replace_env_value(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.with_doc', 'build',
                         '--env__path__data', '/another/path'])
    entry.main()


def test_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.with_param', 'build',
                         'some_value_for_param'])
    entry.main()


def test_no_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.no_doc', 'build'])
    entry.main()


def test_incomplete_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.incomplete_doc', 'build'])
    entry.main()


def test_invalid_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.invalid_doc', 'build'])
    entry.main()


def test_invalid_module_arg(monkeypatch):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'invalid_module'])

    with pytest.raises(ImportError):
        entry.main()


def test_nonexisting_module(monkeypatch):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'some_module.some_function'])

    with pytest.raises(ImportError):
        entry.main()


def test_invalid_function(monkeypatch):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.invalid_function'])

    with pytest.raises(AttributeError):
        entry.main()


def test_undecorated_function(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'test_pkg.entry.plain_function', 'build'])

    entry.main()


def test_undecorated_function_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python',
                         'test_pkg.entry.plain_function_w_param',
                         'some_value_for_param'])

    entry.main()


def test_parse_doc():
    doc = """
    Some description

    Parameters
    ----------
    param : int
        Description
    """

    expected = {'params': {'param': {'desc': 'Description', 'type': 'int'}},
                'summary': ['Some description']}

    assert entry.parse_doc(doc) == expected


def test_parse_doc_if_missing_numpydoc(monkeypatch):
    monkeypatch.setattr(importlib, 'import_module', lambda x: None)

    assert entry.parse_doc("""docstring""") == {'params': {}, 'summary': None}
