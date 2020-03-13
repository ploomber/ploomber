import pytest
import sys

from ploomber import entry


def test_complete_case(monkeypatch, tmp_sample_dir):
    # TODO: why is it importing from relative to this file even though
    # I'm moving to a different one?
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'entry.with_doc', 'build'])
    entry.main()


def test_replace_env_value(monkeypatch, tmp_sample_dir):
    # TODO: why is it importing from relative to this file even though
    # I'm moving to a different one?
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'entry.with_doc', 'build',
                         '--env__path__data', '/another/path'])
    entry.main()


def test_no_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'entry.no_doc', 'build'])
    entry.main()


def test_incomplete_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'entry.incomplete_doc', 'build'])
    entry.main()


def test_invalid_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', 'entry.invalid_doc', 'build'])
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
                        ['python', 'entry.invalid_function'])

    with pytest.raises(AttributeError):
        entry.main()
