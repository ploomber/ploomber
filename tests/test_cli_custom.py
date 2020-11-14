import subprocess
import os
import importlib
import sys
from pathlib import Path
from unittest.mock import Mock

import click
import jupytext
import nbformat
import pytest
from ploomber.cli import (plot, build, parsers, task, report, cli, status,
                          interact)
from ploomber.tasks import notebook
from ploomber import DAG
import ploomber.dag.DAG as dag_module


@pytest.mark.parametrize('cmd', [
    None, 'add', 'build', 'interact', 'new', 'plot', 'report', 'status',
    'task', 'interact'
])
def test_help_commands(cmd):

    elements = ['ploomber']

    if cmd:
        elements.append(cmd)

    subprocess.run(elements + ['--help'], check=True)


@pytest.mark.parametrize(
    'cmd', ['build', 'plot', 'report', 'status', 'task', 'interact'])
def test_help_shows_env_keys(cmd, tmp_nbs):
    res = subprocess.run(['ploomber', 'build', '--help'],
                         stdout=subprocess.PIPE,
                         check=True)

    out = res.stdout.decode()

    assert '--env--sample' in out


@pytest.mark.parametrize(
    'cmd', ['build', 'plot', 'report', 'status', 'task', 'interact'])
def test_help_shows_env_keys_w_entry_point(cmd, tmp_nbs,
                                           add_current_to_sys_path):
    res = subprocess.run(['ploomber', 'build', '-e', 'factory.make', '--help'],
                         stdout=subprocess.PIPE,
                         check=True)

    out = res.stdout.decode()

    assert '--env--sample' in out


def test_build(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.with_doc'])
    build.main()


def test_build_help_shows_docstring(capsys, monkeypatch):
    monkeypatch.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'test_pkg.entry.with_doc', '--help'])

    with pytest.raises(SystemExit) as excinfo:
        build.main()

    out, _ = capsys.readouterr()

    assert not excinfo.value.code
    assert 'This is some description' in out


def test_build_from_directory(monkeypatch, tmp_nbs_no_yaml):
    Path('output').mkdir()
    monkeypatch.setattr(sys, 'argv', ['python', '--entry-point', '.'])
    build.main()


def test_status(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.with_doc'])
    status.main()


@pytest.mark.parametrize(
    'custom_args',
    [[], ['--output', 'custom.png'], ['-o', 'custom.png'], ['--log', 'DEBUG'],
     ['-o', 'custom.png', '--log', 'DEBUG']])
def test_plot(custom_args, monkeypatch, tmp_sample_dir):
    args_defaults = ['python', '--entry-point', 'test_pkg.entry.with_doc']
    monkeypatch.setattr(sys, 'argv', args_defaults + custom_args)
    mock = Mock()
    monkeypatch.setattr(dag_module.DAG, 'plot', mock)
    plot.main()

    mock.assert_called_once()


def test_report(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.with_doc'])
    report.main()


def test_log_enabled(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_doc', '--log', 'INFO'
    ])
    build.main()


def test_interactive_session(tmp_sample_dir):
    res = subprocess.run(
        ['ploomber', 'interact', '--entry-point', 'test_pkg.entry.with_doc'],
        input=b'type(dag)',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    assert 'Out[1]: ploomber.dag.DAG.DAG' in res.stdout.decode()


def test_interactive_session_develop(monkeypatch, tmp_nbs):
    monkeypatch.setattr(sys, 'argv', ['python'])

    mock = Mock(side_effect=['dag["plot"].develop()', 'quit'])

    def mock_jupyter_notebook(args, check):
        nb = jupytext.reads('2 + 2', fmt='py')
        # args: jupyter {app} {path} {other args,...}
        nbformat.write(nb, args[2])

    with monkeypatch.context() as m:
        # NOTE: I tried patching
        # (IPython.terminal.interactiveshell.TerminalInteractiveShell
        # .prompt_for_code)
        # but didn't work, patching builtins input works
        m.setattr('builtins.input', mock)

        m.setattr(notebook.subprocess, 'run', mock_jupyter_notebook)
        m.setattr(notebook, '_save', lambda: True)
        interact.main()

    assert Path('plot.py').read_text().strip() == '2 + 2'


def test_interactive_session_render_fails(monkeypatch, tmp_nbs):
    monkeypatch.setattr(sys, 'argv', ['python'])

    mock = Mock(side_effect=['quit'])

    def fail(self):
        raise Exception

    with monkeypatch.context() as m:
        m.setattr('builtins.input', mock)
        m.setattr(DAG, 'render', fail)
        interact.main()


def test_replace_env_value(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_doc',
        '--env--path--data', '/another/path'
    ])
    build.main()


def test_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_param',
        'some_value_for_param'
    ])
    build.main()


def test_no_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'test_pkg.entry.no_doc'])
    build.main()


def test_incomplete_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'test_pkg.entry.incomplete_doc'])
    build.main()


def test_invalid_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, 'argv', ['python', '--entry-point', 'test_pkg.entry.invalid_doc'])
    build.main()


def test_invalid_module_arg(monkeypatch):
    monkeypatch.setattr(sys, 'argv',
                        ['python', '--entry-point', 'invalid_module'])

    with pytest.raises(ValueError) as excinfo:
        build.main()

    assert 'Invalid module name' in str(excinfo.value)


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
    monkeypatch.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'test_pkg.entry.plain_function'])

    build.main()


def test_undecorated_function_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.plain_function_w_param',
        'some_value_for_param'
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

    # simulate function with __doc__ attr
    class FakeCallable:
        pass

    callable_ = FakeCallable()
    callable_.__doc__ = doc

    expected = {
        'params': {
            'param': {
                'desc': 'Description',
                'type': 'int'
            }
        },
        'summary': 'Docstring: Some description'
    }

    assert parsers._parse_doc(callable_) == expected


@pytest.mark.parametrize('docstring, expected_summary', [
    ['docstring', 'docstring'],
    ['first line\nsecond line\nthird line', 'first line'],
    ['\nsecond line\nthird line', 'second line'],
])
def test_parse_doc_if_missing_numpydoc(docstring, expected_summary,
                                       monkeypatch):
    def _module_not_found(arg):
        raise ModuleNotFoundError

    # simulate numpydoc isn't installed
    monkeypatch.setattr(importlib, 'import_module', _module_not_found)

    # simulate function with __doc__ attr
    class FakeCallable:
        pass

    callable_ = FakeCallable()
    callable_.__doc__ = docstring

    assert (parsers._parse_doc(callable_) == {
        'params': {},
        'summary': expected_summary
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
        ['python', '--entry-point', 'pipeline.yaml', '--env--sample', 'True'])
    build.main()
