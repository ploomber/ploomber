import subprocess
import os
import importlib
import sys
from pathlib import Path

import click
import pytest
from ploomber.cli import plot, build, parsers, task, report, cli


def test_complete_case(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_doc', '--action',
        'build'
    ])
    build.main()


@pytest.mark.parametrize(
    'custom_args',
    [[], ['--output', 'custom.png'], ['-o', 'custom.png'], ['--log', 'DEBUG'],
     ['-o', 'custom.png', '--log', 'DEBUG']])
def test_plot(custom_args, monkeypatch, tmp_sample_dir):
    args_defaults = ['python', '--entry-point', 'test_pkg.entry.with_doc']
    monkeypatch.setattr(sys, 'argv', args_defaults + custom_args)
    plot.main()


def test_report(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.with_doc'])
    report.main()


def test_log_enabled(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_doc', '--action',
        'build', '--log', 'INFO'
    ])
    build.main()


def test_interactive_session(monkeypatch, tmp_sample_dir):
    res = subprocess.run(
        ['ploomber', 'interact', '--entry-point', 'test_pkg.entry.with_doc'],
        input=b'type(dag)',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    assert 'Out[1]: ploomber.dag.DAG.DAG' in res.stdout.decode()


def test_replace_env_value(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_doc', '--action',
        'build', '--env__path__data', '/another/path'
    ])
    build.main()


def test_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_param', '--action',
        'build', 'some_value_for_param'
    ])
    build.main()


def test_no_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.no_doc', '--action', 'build'
    ])
    build.main()


def test_incomplete_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.incomplete_doc', '--action',
        'build'
    ])
    build.main()


def test_invalid_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.invalid_doc', '--action',
        'build'
    ])
    build.main()


def test_invalid_module_arg(monkeypatch):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'invalid_module'])

    with pytest.raises(ImportError):
        build.main()


def test_nonexisting_module(monkeypatch):
    monkeypatch.setattr(
        sys, 'argv', ['python', '--entry-point', 'some_module.some_function'])

    with pytest.raises(ImportError):
        build.main()


def test_invalid_function(monkeypatch):
    monkeypatch.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'test_pkg.entry.invalid_function'])

    with pytest.raises(AttributeError):
        build.main()


def test_undecorated_function(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.plain_function', '--action',
        'build'
    ])

    build.main()


def test_undecorated_function_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.plain_function_w_param',
        '--action', 'build', 'some_value_for_param'
    ])

    build.main()


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

    assert parsers._parse_doc(doc) == expected


def test_parse_doc_if_missing_numpydoc(monkeypatch):
    monkeypatch.setattr(importlib, 'import_module', lambda x: None)

    assert (parsers._parse_doc("""docstring""") == {
        'params': {},
        'summary': None
    })


@pytest.mark.parametrize('custom', [[], ['--partially', 'plot']])
def test_run_spec(custom, monkeypatch, tmp_directory):
    monkeypatch.setattr(click, 'confirm', lambda text: False)
    monkeypatch.setattr(click, 'prompt', lambda text, type: 'my-project')
    cli._new()
    os.chdir('my-project')
    args = ['python', '--entry-point', 'pipeline.yaml'] + custom
    monkeypatch.setattr(sys, 'argv', args)
    build.main()


@pytest.mark.parametrize(
    'custom', [[], ['--source'], ['--source', '--build'], ['--force'],
               ['--force', '--build'], ['--status'], ['--status', '--build']])
def test_task(custom, monkeypatch, tmp_directory):
    monkeypatch.setattr(click, 'confirm', lambda text: False)
    monkeypatch.setattr(click, 'prompt', lambda text, type: 'my-project')
    cli._new()
    os.chdir('my-project')

    args = ['task', '--entry-point', 'pipeline.yaml', 'raw'] + custom
    monkeypatch.setattr(sys, 'argv', args)
    task.main()


def test_run_spec_replace_value(monkeypatch, tmp_directory):
    monkeypatch.setattr(click, 'confirm', lambda text: False)
    monkeypatch.setattr(click, 'prompt', lambda text, type: 'my-project')
    cli._new()
    os.chdir('my-project')
    Path('env.yaml').write_text('sample: false')

    monkeypatch.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'pipeline.yaml', '--env__sample', 'True'])
    build.main()
