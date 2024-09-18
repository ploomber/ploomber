import os
import subprocess
import importlib
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock

import pytest
from debuglater.pydump import debug_dump

from ploomber.cli import plot, build, parsers, task, report, status, interact
from ploomber_cli.cli import cmd_router
from ploomber.cli.parsers import CustomParser
from ploomber import DAG
import ploomber.dag.dag as dag_module

# TODO: optimize some of the tests that build dags by mocking and checking
# that the build function is called with the appropriate args

IS_WINDOWS_PYTHON_3_10 = sys.version_info >= (3, 10) and "win" in sys.platform


def test_cli_does_not_import_main_package(monkeypatch):
    """
    ploomber_cli.cli should NOT import ploomber since it's a heavy package
    and we want the CLI to be responsive. imports should happen inside each
    command
    """
    out = subprocess.check_output(
        [
            "python",
            "-c",
            'import sys; import ploomber_cli.cli; print("ploomber" in sys.modules)',
        ]
    )

    assert out.decode().strip() == "False"


def test_no_options(monkeypatch):
    # when running "ploomber"
    monkeypatch.setattr(sys, "argv", ["ploomber"])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    assert excinfo.value.code == 0


@pytest.mark.parametrize(
    "cmd, suggestion",
    [
        ["execute", "build"],
        ["run", "build"],
        ["bulid", "build"],
        ["example", "examples"],
        ["tsk", "task"],
        ["rport", "report"],
        ["stats", "status"],
        ["inteact", "interact"],
        ["scafold", "scaffold"],
        ["instal", "install"],
        ["plo", "plot"],
    ],
)
def test_suggestions(monkeypatch, capsys, cmd, suggestion):
    monkeypatch.setattr(sys, "argv", ["ploomber", cmd])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()
    assert f"Did you mean '{suggestion}'?" in captured.err
    assert excinfo.value.code == 2


def test_build_suggestion(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["ploomber", "build", "task"])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()
    suggestion = "To build a single task, try: 'ploomber task {task-name}'"
    assert suggestion in captured.err
    assert excinfo.value.code == 2


@pytest.mark.parametrize(
    "cmd",
    [
        None,
        "scaffold",
        "build",
        "interact",
        "plot",
        "report",
        "status",
        "task",
        "interact",
    ],
)
def test_help(cmd, monkeypatch, tmp_directory):
    Path("pipeline.yaml").touch()

    elements = ["ploomber"]

    if cmd:
        elements.append(cmd)

    monkeypatch.setattr(sys, "argv", elements + ["--help"])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    assert not excinfo.value.code


@pytest.mark.parametrize(
    "cmd", ["build", "plot", "report", "status", "task", "interact"]
)
def test_help_shows_env_keys(cmd, monkeypatch, tmp_nbs, capsys):
    monkeypatch.setattr(sys, "argv", ["ploomber", cmd, "--help"])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()
    assert "--env--sample" in captured.out
    assert not excinfo.value.code


@pytest.mark.parametrize(
    "cmd", ["build", "plot", "report", "status", "task", "interact"]
)
def test_help_shows_env_keys_w_entry_point(
    cmd, tmp_nbs, add_current_to_sys_path, monkeypatch, capsys
):
    monkeypatch.setattr(sys, "argv", ["ploomber", cmd, "-e", "factory.make", "--help"])

    with pytest.raises(SystemExit) as excinfo:
        cmd_router()

    captured = capsys.readouterr()
    assert "--env--sample" in captured.out
    assert not excinfo.value.code


def test_build(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.with_doc"]
    )
    build.main(catch_exception=False)


def test_build_help_shows_docstring(capsys, monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.with_doc", "--help"]
    )

    with pytest.raises(SystemExit) as excinfo:
        build.main(catch_exception=False)

    out, _ = capsys.readouterr()

    assert not excinfo.value.code
    assert "This is some description" in out


@pytest.mark.parametrize("arg", [".", "*.py"])
def test_build_from_directory(arg, monkeypatch, tmp_nbs_no_yaml):
    Path("output").mkdir()
    monkeypatch.setattr(sys, "argv", ["python", "--entry-point", arg])
    build.main(catch_exception=False)


def test_status(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.with_doc"]
    )
    status.main(catch_exception=False)


@pytest.mark.skipif(IS_WINDOWS_PYTHON_3_10, reason="requires < 3.10")
@pytest.mark.parametrize(
    "custom_args, output, include_products, backend",
    [
        [[], "pipeline.entry.png", False, None],
        [["--output", "custom.png"], "custom.png", False, None],
        [["-o", "custom.png"], "custom.png", False, None],
        [["--log", "DEBUG"], "pipeline.entry.png", False, None],
        [["-o", "custom.png", "--log", "DEBUG"], "custom.png", False, None],
        [["--include-products"], "pipeline.entry.png", True, None],
        [["-i"], "pipeline.entry.png", True, None],
        [["-b", "d3"], "pipeline.entry.html", False, "d3"],
        [["--backend", "pygraphviz"], "pipeline.entry.png", False, "pygraphviz"],
    ],
)
def test_plot(
    custom_args, monkeypatch, tmp_sample_dir, output, include_products, backend, capsys
):
    args_defaults = ["python", "--entry-point", "test_pkg.entry.with_doc"]
    monkeypatch.setattr(sys, "argv", args_defaults + custom_args)
    mock = Mock()
    monkeypatch.setattr(dag_module.DAG, "plot", mock)

    plot.main(catch_exception=False)
    captured = capsys.readouterr()

    assert output in captured.out
    mock.assert_called_once_with(
        backend=backend, output=output, include_products=include_products
    )


@pytest.mark.skipif(IS_WINDOWS_PYTHON_3_10, reason="requires < 3.10")
def test_plot_uses_name_if_any(tmp_nbs, monkeypatch):
    os.rename("pipeline.yaml", "pipeline.train.yaml")

    args_defaults = ["ploomber", "--entry-point", "pipeline.train.yaml"]
    monkeypatch.setattr(sys, "argv", args_defaults)
    mock = Mock()
    monkeypatch.setattr(dag_module.DAG, "plot", mock)
    plot.main(catch_exception=False)

    mock.assert_called_once_with(
        output="pipeline.train.png", include_products=False, backend=None
    )


def test_report_includes_plot(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.with_doc"]
    )

    mock_plot = Mock()
    monkeypatch.setattr(dag_module.DAG, "plot", mock_plot)
    report.main(catch_exception=False)

    mock_plot.assert_called_once()


def test_log_enabled(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys,
        "argv",
        ["python", "--entry-point", "test_pkg.entry.with_doc", "--log", "INFO"],
    )
    build.main(catch_exception=False)


def test_interactive_session(tmp_sample_dir):
    res = subprocess.run(
        ["ploomber", "interact", "--entry-point", "test_pkg.entry.with_doc"],
        input=b"type(dag)",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert "Out[1]: ploomber.dag.dag.DAG" in res.stdout.decode()


def test_interact_command_starts_full_ipython_session(monkeypatch, tmp_nbs):
    mock_dag = Mock()
    mock_start_ipython = Mock()
    monkeypatch.setattr(sys, "argv", ["interact"])
    monkeypatch.setattr(interact, "start_ipython", mock_start_ipython)
    monkeypatch.setattr(
        interact.CustomParser, "load_from_entry_point_arg", lambda _: (mock_dag, None)
    )

    interact.main(catch_exception=False)

    mock_start_ipython.assert_called_once_with(argv=[], user_ns={"dag": mock_dag})


def test_interactive_session_render_fails(monkeypatch, tmp_nbs):
    mock_dag = Mock(side_effect=["quit"])
    mock_start_ipython = Mock()
    monkeypatch.setattr(sys, "argv", ["python"])
    monkeypatch.setattr(interact, "start_ipython", mock_start_ipython)

    def fail(self):
        raise Exception

    with monkeypatch.context() as m:
        m.setattr("builtins.input", mock_dag)
        m.setattr(DAG, "render", fail)
        interact.main(catch_exception=False)


def test_replace_env_value(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "python",
            "--entry-point",
            "test_pkg.entry.with_doc",
            "--env--path--data",
            "/another/path",
        ],
    )
    build.main(catch_exception=False)


def test_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "python",
            "--entry-point",
            "test_pkg.entry.with_param",
            "some_value_for_param",
        ],
    )
    build.main(catch_exception=False)


def test_no_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.no_doc"]
    )
    build.main(catch_exception=False)


def test_incomplete_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.incomplete_doc"]
    )
    build.main(catch_exception=False)


def test_invalid_doc(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.invalid_doc"]
    )
    build.main(catch_exception=False)


def test_invalid_entry_point_value(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["python", "--entry-point", "invalid_entry_point"])

    with pytest.raises(ValueError) as excinfo:
        build.main(catch_exception=False)

    assert "Could not determine the entry point type" in str(excinfo.value)


@pytest.mark.parametrize(
    "args",
    [
        ["--entry-point", "pipeline.yaml"],
        ["--entry-point", "pipeline.yml"],
    ],
)
def test_invalid_spec_entry_point(args, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["python"] + args)

    with pytest.raises(ValueError) as excinfo:
        build.main(catch_exception=False)

    assert "Could not determine the entry point type" in str(excinfo.value)


def test_nonexisting_module(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "some_module.some_function"]
    )

    with pytest.raises(ImportError):
        build.main(catch_exception=False)


def test_invalid_function(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.invalid_function"]
    )

    with pytest.raises(AttributeError):
        build.main(catch_exception=False)


def test_undecorated_function(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys, "argv", ["python", "--entry-point", "test_pkg.entry.plain_function"]
    )

    build.main(catch_exception=False)


def test_undecorated_function_w_param(monkeypatch, tmp_sample_dir):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "python",
            "--entry-point",
            "test_pkg.entry.plain_function_w_param",
            "some_value_for_param",
        ],
    )

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
        "params": {"param": {"desc": "Description", "type": "int"}},
        "summary": "Docstring: Some description",
    }

    assert parsers._parse_doc(callable_) == expected


@pytest.mark.parametrize(
    "docstring, expected_summary",
    [
        ["docstring", "docstring"],
        ["first line\nsecond line\nthird line", "first line"],
        ["\nsecond line\nthird line", "second line"],
    ],
)
def test_parse_doc_if_missing_numpydoc(docstring, expected_summary, monkeypatch):
    def _module_not_found(arg):
        raise ModuleNotFoundError

    # simulate numpydoc isn't installed
    monkeypatch.setattr(importlib, "import_module", _module_not_found)

    # simulate function with __doc__ attr
    class FakeCallable:
        pass

    callable_ = FakeCallable()
    callable_.__doc__ = docstring

    assert parsers._parse_doc(callable_) == {"params": {}, "summary": expected_summary}


@pytest.mark.parametrize("args", [[], ["--partially", "plot"]])
def test_build_command(args, tmp_nbs, monkeypatch):
    args = ["python", "--entry-point", "pipeline.yaml"] + args
    monkeypatch.setattr(sys, "argv", args)
    build.main(catch_exception=False)


def test_build_crash_hides_internal_traceback(tmp_nbs, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["build"])

    # crash pipeline
    path = Path("load.py")
    path.write_text(path.read_text() + "\nraise Exception")

    with pytest.raises(SystemExit):
        build.main()

    captured = capsys.readouterr()

    assert "ploomber.exceptions.DAGBuildError" not in captured.err


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--source"],
        ["--source", "--build"],
        ["--force"],
        ["--force", "--build"],
        ["--status"],
        ["--status", "--build"],
        ["--on-finish"],
    ],
)
def test_task_command(args, tmp_nbs, monkeypatch, capsys):
    args = ["task", "--entry-point", "pipeline.yaml", "load"] + args
    monkeypatch.setattr(sys, "argv", args)
    print("test task here!!")
    task.main(catch_exception=False)
    captured = capsys.readouterr()
    if args == "--source" or args == "--status" or args == "--on-finish":
        assert "executed successfully!" in captured.out
        assert "Products" in captured.out


def test_task_command_does_not_force_dag_render(tmp_nbs, monkeypatch):
    """
    Make sure the force flag is only used in task.render and not dag.render
    because we don't want to override the status of other tasks
    """
    args = ["task", "load", "--force"]
    monkeypatch.setattr(sys, "argv", args)

    class CustomParserWrapper(CustomParser):
        def load_from_entry_point_arg(self):
            dag, args = super().load_from_entry_point_arg()
            dag_mock = MagicMock(wraps=dag)
            type(self).dag_mock = dag_mock
            return dag_mock, args

    monkeypatch.setattr(task, "CustomParser", CustomParserWrapper)

    task.main(catch_exception=False)

    CustomParserWrapper.dag_mock.render.assert_called_once_with()


def test_build_with_replaced_env_value(tmp_nbs, monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["python", "--entry-point", "pipeline.yaml", "--env--sample", "True"],
    )
    build.main(catch_exception=False)


def test_task_command_formats_exception(
    tmp_directory, monkeypatch, tmp_imports, capsys
):
    Path("functions.py").write_text(
        """
def load(product):
    raise ValueError
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: functions.load
      product: output.txt
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "task", "load"])

    with pytest.raises(SystemExit):
        cmd_router()

    captured = capsys.readouterr()
    expected = (
        "raise ValueError\nValueError\n\nploomber.exceptions."
        'TaskBuildError: Error building task "load"'
    )
    assert expected in captured.err


@pytest.mark.parametrize("output", [None, "pipeline.html", "pipeline.png"])
@pytest.mark.parametrize("backend", [None, "d3", "pygraphviz"])
def test_report_command(tmp_nbs, backend, output, monkeypatch, capsys):
    args = [
        "ploomber",
        "report",
        "--entry-point",
        "pipeline.yaml",
        "--backend",
        backend,
    ]
    if output:
        args.extend(["--output", output])
    monkeypatch.setattr(sys, "argv", args)
    output = "pipeline.html" if not output else output
    cmd_router()
    expected_output_path = Path(output)
    captured = capsys.readouterr()

    assert expected_output_path.exists()
    assert f"saved at: {output}" in captured.out


def test_build_debug_now_python_callable(tmp_directory, monkeypatch, tmp_imports):
    Path("functions.py").write_text(
        """
def crash(product):
    x, y = 1, 0
    x/y
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: functions.crash
      product: out.txt
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "build", "--debug"])

    mock = Mock()

    with pytest.raises(SystemExit):
        with monkeypatch.context() as m:
            m.setitem(sys.modules, "ipdb", mock)
            cmd_router()

    tb = mock.post_mortem.call_args[0][0]
    assert type(tb).__name__ == "traceback"


def test_build_debug_later_python_callable(
    tmp_directory, monkeypatch, tmp_imports, capsys
):
    Path("functions.py").write_text(
        """
def crash(product):
    x, y = 1, 0
    x/y
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: functions.crash
      product: out.txt
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "build", "--debuglater"])

    with pytest.raises(SystemExit):
        cmd_router()

    captured = capsys.readouterr()

    assert Path("crash.dump").is_file()
    assert "dltr crash.dump" in captured.err


@pytest.mark.skip
def test_build_debug_now_notebook(tmp_directory, monkeypatch, tmp_imports):
    Path("crash.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
x, y = 1, 0
x / y
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: crash.py
      product: out.ipynb
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "build", "--debug"])

    mock = Mock(side_effect=["print(f'x={x}')", "quit"])

    with pytest.raises(SystemExit):
        with monkeypatch.context() as m:
            m.setattr("builtins.input", mock)
            cmd_router()


def test_build_debug_later_notebook(tmp_directory, monkeypatch, tmp_imports, capsys):
    Path("crash.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
x, y = 1, 0
x / y
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: crash.py
      product: out.ipynb
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "build", "--debuglater"])

    with pytest.raises(SystemExit):
        cmd_router()

    captured = capsys.readouterr()
    assert "dltr crash.dump" in captured.err

    mock = Mock(side_effect=["print(f'x={x}')", "quit"])

    with monkeypatch.context() as m:
        m.setattr("builtins.input", mock)
        debug_dump("crash.dump")


def test_task_debug_now_python_callable(tmp_directory, monkeypatch, tmp_imports):
    Path("functions.py").write_text(
        """
def crash(product):
    x, y = 1, 0
    x/y
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: functions.crash
      product: out.txt
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "task", "crash", "--debug"])

    mock = Mock()

    with pytest.raises(SystemExit):
        with monkeypatch.context() as m:
            m.setitem(sys.modules, "ipdb", mock)
            cmd_router()

    tb = mock.post_mortem.call_args[0][0]
    assert type(tb).__name__ == "traceback"


def test_task_debug_later_python_callable(
    tmp_directory, monkeypatch, tmp_imports, capsys
):
    Path("functions.py").write_text(
        """
def crash(product):
    x, y = 1, 0
    x/y
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: functions.crash
      product: out.txt
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "task", "crash", "--debuglater"])

    with pytest.raises(SystemExit):
        cmd_router()

    captured = capsys.readouterr()

    assert Path("crash.dump").is_file()
    assert "dltr crash.dump" in captured.err


@pytest.mark.skip
def test_task_debug_now_notebook(tmp_directory, monkeypatch, tmp_imports):
    Path("crash.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
x, y = 1, 0
x / y
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: crash.py
      product: out.ipynb
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "task", "crash", "--debug"])

    mock = Mock(side_effect=["print(f'x={x}')", "quit"])

    with pytest.raises(SystemExit):
        with monkeypatch.context() as m:
            m.setattr("builtins.input", mock)
            cmd_router()


def test_task_debug_later_notebook(tmp_directory, monkeypatch, tmp_imports, capsys):
    Path("crash.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
x, y = 1, 0
x / y
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: crash.py
      product: out.ipynb
"""
    )

    monkeypatch.setattr(sys, "argv", ["ploomber", "task", "crash", "--debuglater"])

    with pytest.raises(SystemExit):
        cmd_router()

    captured = capsys.readouterr()
    assert "dltr crash.dump" in captured.err

    mock = Mock(side_effect=["print(f'x={x}')", "quit"])

    with monkeypatch.context() as m:
        m.setattr("builtins.input", mock)
        debug_dump("crash.dump")
