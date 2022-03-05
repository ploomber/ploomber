import os
import subprocess
import importlib
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock

import jupytext
import nbformat
import pytest

from ploomber.cli import plot, build, parsers, task, report, status, interact
from ploomber.cli.cli import cmd_router
from ploomber.cli.parsers import CustomParser
from ploomber.tasks import notebook
from ploomber import DAG
import ploomber.dag.dag as dag_module
# TODO: optimize some of the tests that build dags by mocking and checking
# that the build function is called with the appropriate args
from ploomber.telemetry import telemetry


def test_no_options(monkeypatch):
    # when running "ploomber"
    monkeypatch.setattr(sys, 'argv', ['ploomber'])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    assert excinfo.value.code == 0


@pytest.mark.parametrize('cmd, suggestion', [
    ['execute', 'build'],
    ['run', 'build'],
    ['bulid', 'build'],
    ['buld', 'build'],
    ['bild', 'build'],
    ['uild', 'build'],
    ['buil', 'build'],
    ['example', 'examples'],
    ['exemples', 'examples'],
    ['exmples', 'examples'],
    ['exampes', 'examples'],
    ['tsk', 'task'],
    ['tas', 'task'],
    ['rport', 'report'],
    ['reprt', 'report'],
    ['repor', 'report'],
    ['stat', 'status'],
    ['stats', 'status'],
    ['satus', 'status'],
    ['inteact', 'interact'],
    ['interat', 'interact'],
])
def test_suggestions(monkeypatch, capsys, cmd, suggestion):
    monkeypatch.setattr(sys, 'argv', ['ploomber', cmd])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()
    assert f"Did you mean '{suggestion}'?" in captured.err
    assert excinfo.value.code == 2


def test_build_suggestion(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'build', 'task'])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()
    suggestion = "To build a single task, try: 'ploomber task {task-name}'"
    assert suggestion in captured.err
    assert excinfo.value.code == 2


@pytest.mark.parametrize('cmd', [
    None,
    'scaffold',
    'build',
    'interact',
    'plot',
    'report',
    'status',
    'task',
    'interact',
])
def test_help(cmd, monkeypatch_session, tmp_directory):
    Path('pipeline.yaml').touch()

    elements = ['ploomber']

    if cmd:
        elements.append(cmd)

    monkeypatch_session.setattr(sys, 'argv', elements + ['--help'])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    assert not excinfo.value.code


@pytest.mark.parametrize(
    'cmd', ['build', 'plot', 'report', 'status', 'task', 'interact'])
def test_help_shows_env_keys(cmd, monkeypatch_session, tmp_nbs, capsys):
    monkeypatch_session.setattr(sys, 'argv', ['ploomber', cmd, '--help'])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()
    assert '--env--sample' in captured.out
    assert not excinfo.value.code


@pytest.mark.parametrize(
    'cmd', ['build', 'plot', 'report', 'status', 'task', 'interact'])
def test_help_shows_env_keys_w_entry_point(cmd, tmp_nbs,
                                           add_current_to_sys_path,
                                           monkeypatch_session, capsys):
    monkeypatch_session.setattr(
        sys, 'argv', ['ploomber', cmd, '-e', 'factory.make', '--help'])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()
    assert '--env--sample' in captured.out
    assert not excinfo.value.code


def test_build(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(
        sys, 'argv', ['python', '--entry-point', 'test_pkg.entry.with_doc'])
    build.main(catch_exception=False)


def test_build_help_shows_docstring(capsys, monkeypatch_session):
    monkeypatch_session.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'test_pkg.entry.with_doc', '--help'])

    with pytest.raises(SystemExit) as excinfo:
        build.main(catch_exception=False)

    out, _ = capsys.readouterr()

    assert not excinfo.value.code
    assert 'This is some description' in out


@pytest.mark.parametrize('arg', ['.', '*.py'])
def test_build_from_directory(arg, monkeypatch_session, tmp_nbs_no_yaml):
    Path('output').mkdir()
    monkeypatch_session.setattr(sys, 'argv', ['python', '--entry-point', arg])
    build.main(catch_exception=False)


def test_status(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(
        sys, 'argv', ['python', '--entry-point', 'test_pkg.entry.with_doc'])
    status.main(catch_exception=False)


@pytest.mark.parametrize(
    'custom_args',
    [[], ['--output', 'custom.png'], ['-o', 'custom.png'], ['--log', 'DEBUG'],
     ['-o', 'custom.png', '--log', 'DEBUG']])
def test_plot(custom_args, monkeypatch_session, tmp_sample_dir):
    args_defaults = ['python', '--entry-point', 'test_pkg.entry.with_doc']
    monkeypatch_session.setattr(sys, 'argv', args_defaults + custom_args)
    mock = Mock()
    monkeypatch_session.setattr(dag_module.DAG, 'plot', mock)
    plot.main(catch_exception=False)

    mock.assert_called_once()


def test_plot_uses_name_if_any(tmp_nbs, monkeypatch_session):
    os.rename('pipeline.yaml', 'pipeline.train.yaml')

    args_defaults = ['ploomber', '--entry-point', 'pipeline.train.yaml']
    monkeypatch_session.setattr(sys, 'argv', args_defaults)
    mock = Mock()
    monkeypatch_session.setattr(dag_module.DAG, 'plot', mock)
    plot.main(catch_exception=False)

    mock.assert_called_once_with(output='pipeline.train.png')


def test_report_includes_plot(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(
        sys, 'argv', ['python', '--entry-point', 'test_pkg.entry.with_doc'])

    mock_plot = Mock()
    monkeypatch_session.setattr(dag_module.DAG, 'plot', mock_plot)
    report.main(catch_exception=False)

    mock_plot.assert_called_once()


def test_log_enabled(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_doc', '--log', 'INFO'
    ])
    build.main(catch_exception=False)


@pytest.mark.skip(reason="Skipping test so it won't call log_api")
def test_interactive_session(tmp_sample_dir, monkeypatch_session):
    mock_log = Mock()
    monkeypatch_session.setattr(telemetry, 'log_api', mock_log)
    res = subprocess.run(
        ['ploomber', 'interact', '--entry-point', 'test_pkg.entry.with_doc'],
        input=b'type(dag)',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    assert 'Out[1]: ploomber.dag.dag.DAG' in res.stdout.decode()


def test_interact_command_starts_full_ipython_session(monkeypatch, tmp_nbs):
    mock_dag = Mock()
    mock_log = Mock()
    mock_start_ipython = Mock()
    monkeypatch.setattr(sys, 'argv', ['interact'])
    monkeypatch.setattr(interact, 'start_ipython', mock_start_ipython)
    monkeypatch.setattr(telemetry, 'log_api', mock_log)
    monkeypatch.setattr(interact.CustomParser, 'load_from_entry_point_arg',
                        lambda _: (mock_dag, None))

    interact.main(catch_exception=False)

    mock_start_ipython.assert_called_once_with(argv=[],
                                               user_ns={'dag': mock_dag})


def test_interactive_session_develop(monkeypatch_session, tmp_nbs):
    monkeypatch_session.setattr(sys, 'argv', ['python'])

    mock = Mock(side_effect=['dag["plot"].develop()', 'quit'])

    def mock_jupyter_notebook(args, check):
        nb = jupytext.reads('2 + 2', fmt='py')
        # args: jupyter {app} {path} {other args,...}
        nbformat.write(nb, args[2])

    with monkeypatch_session.context() as m:
        # NOTE: I tried patching
        # (IPython.terminal.interactiveshell.TerminalInteractiveShell
        # .prompt_for_code)
        # but didn't work, patching builtins input works
        m.setattr('builtins.input', mock)

        m.setattr(notebook.subprocess, 'run', mock_jupyter_notebook)
        m.setattr(notebook, '_save', lambda: True)
        interact.main(catch_exception=False)

    assert Path('plot.py').read_text().strip() == '2 + 2'


def test_interactive_session_render_fails(monkeypatch_session, tmp_nbs):
    monkeypatch_session.setattr(sys, 'argv', ['python'])

    mock = Mock(side_effect=['quit'])

    def fail(self):
        raise Exception

    with monkeypatch_session.context() as m:
        m.setattr('builtins.input', mock)
        m.setattr(DAG, 'render', fail)
        interact.main(catch_exception=False)


def test_replace_env_value(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_doc',
        '--env--path--data', '/another/path'
    ])
    build.main(catch_exception=False)


def test_w_param(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.with_param',
        'some_value_for_param'
    ])
    build.main(catch_exception=False)


def test_no_doc(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(
        sys, 'argv', ['python', '--entry-point', 'test_pkg.entry.no_doc'])
    build.main(catch_exception=False)


def test_incomplete_doc(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'test_pkg.entry.incomplete_doc'])
    build.main(catch_exception=False)


def test_invalid_doc(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(
        sys, 'argv', ['python', '--entry-point', 'test_pkg.entry.invalid_doc'])
    build.main(catch_exception=False)


def test_invalid_entry_point_value(monkeypatch_session):
    monkeypatch_session.setattr(
        sys, 'argv', ['python', '--entry-point', 'invalid_entry_point'])

    with pytest.raises(ValueError) as excinfo:
        build.main(catch_exception=False)

    assert 'Could not determine the entry point type' in str(excinfo.value)


@pytest.mark.parametrize('args', [
    ['--entry-point', 'pipeline.yaml'],
    ['--entry-point', 'pipeline.yml'],
])
def test_invalid_spec_entry_point(args, monkeypatch_session):
    monkeypatch_session.setattr(sys, 'argv', ['python'] + args)

    with pytest.raises(ValueError) as excinfo:
        build.main(catch_exception=False)

    assert 'Could not determine the entry point type' in str(excinfo.value)


def test_nonexisting_module(monkeypatch_session):
    monkeypatch_session.setattr(
        sys, 'argv', ['python', '--entry-point', 'some_module.some_function'])

    with pytest.raises(ImportError):
        build.main(catch_exception=False)


def test_invalid_function(monkeypatch_session):
    monkeypatch_session.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'test_pkg.entry.invalid_function'])

    with pytest.raises(AttributeError):
        build.main(catch_exception=False)


def test_undecorated_function(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'test_pkg.entry.plain_function'])

    build.main(catch_exception=False)


def test_undecorated_function_w_param(monkeypatch_session, tmp_sample_dir):
    monkeypatch_session.setattr(sys, 'argv', [
        'python', '--entry-point', 'test_pkg.entry.plain_function_w_param',
        'some_value_for_param'
    ])

    build.main(catch_exception=False)


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


@pytest.mark.parametrize('args', [[], ['--partially', 'plot']])
def test_build_command(args, tmp_nbs, monkeypatch_session):
    args = ['python', '--entry-point', 'pipeline.yaml'] + args
    monkeypatch_session.setattr(sys, 'argv', args)
    build.main(catch_exception=False)


def test_build_crash_hides_internal_traceback(tmp_nbs, monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['build'])

    # crash pipeline
    path = Path('load.py')
    path.write_text(path.read_text() + '\nraise Exception')

    with pytest.raises(SystemExit):
        build.main()

    captured = capsys.readouterr()

    assert 'ploomber.exceptions.DAGBuildError' not in captured.err


@pytest.mark.parametrize('args', [
    [],
    ['--source'],
    ['--source', '--build'],
    ['--force'],
    ['--force', '--build'],
    ['--status'],
    ['--status', '--build'],
    ['--on-finish'],
])
def test_task_command(args, tmp_nbs, monkeypatch_session):
    args = ['task', '--entry-point', 'pipeline.yaml', 'load'] + args
    monkeypatch_session.setattr(sys, 'argv', args)
    task.main(catch_exception=False)


def test_task_command_does_not_force_dag_render(tmp_nbs, monkeypatch_session):
    """
    Make sure the force flag is only used in task.render and not dag.render
    because we don't want to override the status of other tasks
    """
    args = ['task', 'load', '--force']
    monkeypatch_session.setattr(sys, 'argv', args)

    class CustomParserWrapper(CustomParser):
        def load_from_entry_point_arg(self):
            dag, args = super().load_from_entry_point_arg()
            dag_mock = MagicMock(wraps=dag)
            type(self).dag_mock = dag_mock
            return dag_mock, args

    monkeypatch_session.setattr(task, 'CustomParser', CustomParserWrapper)

    task.main(catch_exception=False)

    CustomParserWrapper.dag_mock.render.assert_called_once_with()


def test_build_with_replaced_env_value(tmp_nbs, monkeypatch_session):
    monkeypatch_session.setattr(
        sys, 'argv',
        ['python', '--entry-point', 'pipeline.yaml', '--env--sample', 'True'])
    build.main(catch_exception=False)


def test_task_command_formats_exception(tmp_directory, monkeypatch,
                                        tmp_imports, capsys):
    Path('functions.py').write_text("""
def load(product):
    raise ValueError
""")

    Path('pipeline.yaml').write_text("""
tasks:
    - source: functions.load
      product: output.txt
""")

    monkeypatch.setattr(sys, 'argv', ['ploomber', 'task', 'load'])

    with pytest.raises(SystemExit):
        cmd_router()

    captured = capsys.readouterr()
    expected = ('raise ValueError\nValueError\n\nploomber.exceptions.'
                'TaskBuildError: Error building task "load"')
    assert expected in captured.err
