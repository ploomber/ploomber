"""
Tests for the custom jupyter contents manager
"""
import sys
import os
from pathlib import Path
from unittest.mock import Mock, call
import shutil

import yaml
from ipython_genutils.tempdir import TemporaryDirectory
from notebook.services.contents.tests.test_manager import TestContentsManager
from notebook.notebookapp import NotebookApp
import jupytext
import parso
import nbformat
import pytest
from jupyter_server import serverapp

from ploomber import DAG
from ploomber.jupyter.manager import derive_class
from ploomber.jupyter.dag import JupyterDAGManager
from ploomber.spec import DAGSpec

from jupytext.contentsmanager import TextFileContentsManager

PloomberContentsManager = derive_class(TextFileContentsManager)


class PloomberContentsManagerTestCase(TestContentsManager):
    """
    This runs the original test suite from jupyter to make sure our
    content manager works ok

    https://github.com/jupyter/notebook/blob/b152dd314decda6edbaee1756bb6f6fc50c50f9f/notebook/services/contents/tests/test_manager.py#L218

    Docs: https://jupyter-server.readthedocs.io/en/stable/developers/contents.html#testing
    """  # noqa

    def setUp(self):
        self._temp_dir = TemporaryDirectory()
        self.td = self._temp_dir.name
        self.contents_manager = PloomberContentsManager(root_dir=self.td)


# the following tests check our custom logic


def get_injected_cell(nb):
    injected = None

    for cell in nb["cells"]:
        if "injected-parameters" in cell["metadata"].get("tags", []):
            injected = cell

    return injected


class YAML:
    def __init__(self, path):
        self.path = Path(path)

        if not self.path.exists():
            self.data = dict()
        else:
            self.data = self.read()

    def write(self):
        self.path.write_text(yaml.dump(self.data))

    def read(self):
        self.data = yaml.safe_load(self.path.read_text())
        return self.data


def test_manager_initialization(tmp_directory):
    """
    There some weird stuff in the Jupyter contents manager base class that
    causses the initialization to break if we modify the __init__ method
    in PloomberContentsManager. We add this to check that values are
    correctly initialized
    """
    dir_ = Path("some_dir")
    dir_.mkdir()
    dir_ = dir_.resolve()
    dir_ = str(dir_)

    app = serverapp.ServerApp()
    app.initialize(argv=[])
    app.root_dir = dir_
    assert app.contents_manager.root_dir == dir_


def test_cell_injection_if_using_notebook_dir_option(tmp_nbs):
    Path("jupyter_init").mkdir()
    os.chdir("jupyter_init")

    app = serverapp.ServerApp()
    app.initialize(argv=[])
    # simulate doing: jupyter lab --notebook-dir path/to/dir
    app.root_dir = str(tmp_nbs.resolve())

    model = app.contents_manager.get(str("plot.py"))

    assert get_injected_cell(model["content"])


def test_does_not_log_error_dag_when_getting_a_directory(tmp_directory, capsys):
    Path("pipeline.yaml").touch()

    app = serverapp.ServerApp()
    app.initialize(argv=[])

    # jupyter refreshes the current directory every few seconds
    # (by calling.get('')), we simulate that here
    app.contents_manager.get("")
    app.contents_manager.get("")

    captured = capsys.readouterr()
    lines = captured.err.splitlines()
    log_with_skip_message = [
        line for line in lines if "[Ploomber] An error occured" in line
    ]
    assert not len(log_with_skip_message)


def test_does_not_log_skip_dag_when_getting_a_directory(tmp_directory, capsys):
    app = serverapp.ServerApp()
    app.initialize(argv=[])

    # jupyter refreshes the current directory every few seconds
    # (by calling.get('')), we simulate that here
    app.contents_manager.get("")
    app.contents_manager.get("")

    captured = capsys.readouterr()
    lines = captured.err.splitlines()

    log_with_skip_message = [
        line for line in lines if "[Ploomber] Skipping DAG initialization" in line
    ]
    assert not len(log_with_skip_message)


def test_does_not_log_again_if_using_the_same_spec(tmp_nbs):
    # NOTE: unlike other tests that check logs, we're not using the capsys
    # fixture here. For some reason, the output isn't captured.
    app = serverapp.ServerApp()
    app.initialize(argv=[])
    app.contents_manager.log.info = Mock()

    app.contents_manager.get("plot.py")

    # Jupyter constantly requests the current file, so we simulate here
    app.contents_manager.get("plot.py")

    lines = [call[0][0] for call in app.contents_manager.log.info.call_args_list]

    log_messages = [
        line for line in lines if "[Ploomber] Using dag defined at:" in line
    ]

    # since the spec is still the same, we should not log the spec info again
    # and there must only be one record to log the spec location
    assert len(log_messages) == 1


def test_logs_if_spec_keys_change(tmp_directory):
    Path("one.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
1 + 1
"""
    )

    Path("another.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
1 + 1
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: one.py
      product: one.ipynb
"""
    )

    app = serverapp.ServerApp()
    app.initialize(argv=[])
    app.contents_manager.log.info = Mock()

    app.contents_manager.get("one.py")

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: one.py
      product: one.ipynb
    - source: another.py
      product: another.ipynb
"""
    )

    app.contents_manager.get("one.py")

    lines = [call[0][0] for call in app.contents_manager.log.info.call_args_list]

    log_messages = [
        line for line in lines if "[Ploomber] Using dag defined at:" in line
    ]

    assert len(log_messages) == 2


def test_logs_if_spec_location_changes(tmp_directory):
    Path("one").mkdir()
    Path("another").mkdir()

    Path("script.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
1 + 1
"""
    )

    Path("one", "one.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
1 + 1
"""
    )

    Path("another", "another.py").write_text(
        """
# + tags=["parameters"]
upstream = None

# +
1 + 1
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: script.py
      product: another.ipynb
"""
    )

    Path("one", "pipeline.yaml").write_text(
        """
tasks:
    - source: ../script.py
      product: one.ipynb
"""
    )
    Path("another", "pipeline.yaml").write_text(
        """
tasks:
    - source: ../script.py
      product: another.ipynb
"""
    )

    app = serverapp.ServerApp()
    app.initialize(argv=[])
    app.contents_manager.log.info = Mock()

    app.contents_manager.get("one/one.py")
    app.contents_manager.get("another/another.py")

    lines = [call[0][0] for call in app.contents_manager.log.info.call_args_list]

    log_messages = [
        line for line in lines if "[Ploomber] Using dag defined at:" in line
    ]

    assert len(log_messages) == 3


def test_cell_injection_if_using_notebook_dir_option_nested_script(tmp_nbs):
    Path("jupyter_init").mkdir()
    os.chdir("jupyter_init")

    app = serverapp.ServerApp()
    app.initialize(argv=[])
    # simulate doing: jupyter lab --notebook-dir path/to/dir
    app.root_dir = str(Path(tmp_nbs).resolve().parent)

    model = app.contents_manager.get(str("content/plot.py"))

    assert get_injected_cell(model["content"])


def test_injects_cell_if_file_in_dag(tmp_nbs):
    def resolve(path):
        return str(Path(".").resolve() / path)

    cm = PloomberContentsManager()
    model = cm.get("plot.py")

    injected = get_injected_cell(model["content"])

    assert injected

    upstream_expected = {
        "clean": {
            "nb": resolve("output/clean.ipynb"),
            "data": resolve("output/clean.csv"),
        }
    }
    product_expected = resolve("output/plot.ipynb")

    upstream = None
    product = None

    for node in parso.parse(injected["source"]).children:
        code = node.get_code()
        if "upstream" in code:
            upstream = code.split("=")[1]
        elif "product" in code:
            product = code.split("=")[1]

    assert upstream_expected == eval(upstream)
    assert product_expected == eval(product)


def test_injects_cell_even_if_pipeline_yaml_in_subdirectory(tmp_nbs):
    os.chdir("..")
    cm = PloomberContentsManager()
    model = cm.get(str("content/plot.py"))
    injected = get_injected_cell(model["content"])
    assert injected


def test_injects_cell_even_if_there_are_dag_level_hooks(tmp_nbs, tmp_imports):
    Path("hooks.py").write_text(
        """
def some_hook():
    pass
"""
    )

    Path("pipeline.yaml").write_text(
        """
on_render: hooks.some_hook
on_failure: hooks.some_hook
on_finish: hooks.some_hook

tasks:
  - source: load.py
    product:
      nb: output/load.ipynb
      data: output/data.csv
"""
    )

    cm = PloomberContentsManager()

    model = cm.get(str("load.py"))
    assert get_injected_cell(model["content"])


def test_injects_cell_even_if_there_are_task_level_hooks(tmp_nbs, tmp_imports):
    Path("hooks.py").write_text(
        """
def some_hook():
    pass
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
  - source: load.py
    product:
      nb: output/load.ipynb
      data: output/data.csv
    on_render: hooks.some_hook
    on_failure: hooks.some_hook
    on_finish: hooks.some_hook
"""
    )

    cm = PloomberContentsManager()

    model = cm.get(str("load.py"))
    assert get_injected_cell(model["content"])


def test_dag_from_directory(monkeypatch, tmp_nbs):
    # remove files we don't need for this test case
    Path("pipeline.yaml").unlink()
    Path("factory.py").unlink()

    monkeypatch.setenv("ENTRY_POINT", ".")

    cm = PloomberContentsManager()
    model = cm.get("plot.py")
    injected = get_injected_cell(model["content"])
    assert injected


def test_switch_env_file(tmp_directory, monkeypatch):
    Path("script.py").write_text(
        """
# + tags=['parameters']
upstream = None
some_param = None

# +
1 + 1
"""
    )

    Path("env.yaml").write_text(
        """
some_param: value
"""
    )

    Path("env.another.yaml").write_text(
        """
some_param: another
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: script.py
      product: out.ipynb
      params:
        some_param: '{{some_param}}'
"""
    )

    cm = PloomberContentsManager()

    model = cm.get("script.py")
    injected = get_injected_cell(model["content"])
    assert 'param = "value"' in injected["source"]

    monkeypatch.setenv("PLOOMBER_ENV_FILENAME", "env.another.yaml")
    model = cm.get("script.py")
    injected = get_injected_cell(model["content"])
    assert 'param = "another"' in injected["source"]


# TODO: turn fns as notebooks ON, then ensure that it correctly
# imports the module in pipeline.yaml even if there is already
# a module with such a name in sys.modules
def test_ignores_module_in_cache_when_importing_functions():
    pass


def test_ignores_function_tasks_if_fns_as_nbs_is_turned_off(
    tmp_fns_and_scripts, tmp_imports
):
    path = Path("project", "fns_and_scripts.py")
    source = path.read_text()
    # add a import to a non existing module to make the test fail if it
    # attempts to import
    path.write_text("import not_a_module\n" + source)

    cm = PloomberContentsManager()
    model = cm.get("project/another.py")["content"]

    # should not import module
    assert "fns_and_scripts" not in sys.modules
    # but cell injection should still work
    assert get_injected_cell(model)


def test_loads_functions_from_the_appropriate_directory(
    tmp_fns_and_scripts, tmp_imports
):
    cm = PloomberContentsManager()
    assert cm.get("project/another.py")
    assert callable(cm.dag["get"].source.primitive)


def test_dag_from_dotted_path(
    monkeypatch, tmp_nbs, add_current_to_sys_path, no_sys_modules_cache
):
    monkeypatch.setenv("ENTRY_POINT", "factory.make")

    cm = PloomberContentsManager()
    model = cm.get("plot.py")
    injected = get_injected_cell(model["content"])
    assert injected


@pytest.mark.parametrize(
    "cwd, file_to_get",
    [
        [".", "clean/clean.py"],
        ["clean", "clean.py"],
    ],
    ids=["from_root", "from_subdirectory"],
)
def test_dag_from_env_var_with_custom_name(
    monkeypatch, tmp_nbs_nested, cwd, file_to_get
):
    monkeypatch.setenv("ENTRY_POINT", "pipeline.another.yaml")

    os.chdir(cwd)

    cm = PloomberContentsManager()
    model = cm.get(file_to_get)
    assert get_injected_cell(model["content"])


def test_save(tmp_nbs):
    cm = PloomberContentsManager()
    model = cm.get("plot.py")

    # I found a bug when saving a .py file in jupyter notebook: the model
    # received by .save does not have a path, could not reproduce this issue
    # when running this test so I'm deleting it on purpose to simulate that
    # behavior - not sure why this is happening
    del model["path"]

    source = model["content"]["cells"][0]["source"]
    model["content"]["cells"][0]["source"] = "# modification\n" + source
    cm.save(model, path="/plot.py")

    nb = jupytext.read("plot.py")
    code = Path("plot.py").read_text()
    assert get_injected_cell(nb) is None
    assert "# modification" in code


def test_does_not_delete_injected_cell_on_save_if_manually_injected(tmp_nbs):
    dag = DAGSpec("pipeline.yaml").to_dag().render()
    dag["load"].source.save_injected_cell()

    cm = PloomberContentsManager()
    model = cm.get("load.py")
    cm.save(model, path="/load.py")

    nb = jupytext.read("load.py")
    assert get_injected_cell(nb)


def test_deletes_metadata_on_save(tmp_nbs):
    Path("output").mkdir()
    metadata = Path("output/.plot.ipynb.metadata")
    metadata.touch()

    cm = PloomberContentsManager()
    model = cm.get("plot.py")
    cm.save(model, path="/plot.py")

    assert not metadata.exists()


def test_deletes_metadata_on_save_for_file_used_multiple_times(tmp_directory):
    Path("my-task.py").write_text(
        """
# + tags=['parameters']
upstream = None
param = None

# +
1 + 1
"""
    )

    # generate two tasks with the same script (but different params)
    spec = {
        "tasks": [
            {
                "source": "my-task.py",
                "name": "my-task-",
                "product": "my-task.ipynb",
                "grid": {"param": [1, 2]},
            }
        ]
    }

    Path("pipeline.yaml").write_text(yaml.dump(spec))

    m1 = Path(".my-task-0.ipynb.metadata")
    m2 = Path(".my-task-1.ipynb.metadata")
    m1.touch()
    m2.touch()

    cm = PloomberContentsManager()
    model = cm.get("my-task.py")
    cm.save(model, path="/my-task.py")

    assert not m1.exists()
    assert not m2.exists()


def test_skips_if_file_not_in_dag(tmp_nbs):
    cm = PloomberContentsManager()
    model = cm.get("dummy.py")
    nb = jupytext.read("dummy.py")

    # this file is not part of the pipeline, the contents manager should not
    # inject cells
    assert len(model["content"]["cells"]) == len(nb.cells)


def test_import(tmp_nbs):
    # make sure we are able to import modules in the current working
    # directory
    Path("pipeline.yaml").unlink()
    os.rename("pipeline-w-location.yaml", "pipeline.yaml")
    PloomberContentsManager()


def test_injects_cell_when_initialized_from_sub_directory(tmp_nbs_nested):
    # simulate initializing from a directory where we have to recursively
    # look for pipeline.yaml
    os.chdir("load")

    cm = PloomberContentsManager()
    model = cm.get("load.py")

    injected = get_injected_cell(model["content"])
    assert injected


def test_shows_error_if_dag_fails_to_render(tmp_nbs, monkeypatch):
    cm = PloomberContentsManager()

    mock_render = Mock(side_effect=Exception("some error"))
    mock_log, mock_reset = Mock(), Mock(wraps=cm.reset_dag)
    monkeypatch.setattr(DAG, "render", mock_render)
    monkeypatch.setattr(cm.log, "exception", mock_log)
    monkeypatch.setattr(cm, "reset_dag", mock_reset)

    # should catch the exception
    cm.get("load.py")

    msg = (
        "[Ploomber] An error ocurred when rendering your DAG, cells won't "
        "be injected until the issue is resolved"
    )
    cm.log.exception.assert_called_with(msg)
    cm.reset_dag.assert_called()


def test_hot_reload(tmp_nbs):
    # modify base pipeline.yaml to enable hot reload
    with open("pipeline.yaml") as f:
        spec = yaml.load(f, Loader=yaml.SafeLoader)

    spec["meta"]["jupyter_hot_reload"] = True
    spec["meta"]["extract_upstream"] = True

    for t in spec["tasks"]:
        t.pop("upstream", None)

    with open("pipeline.yaml", "w") as f:
        yaml.dump(spec, f)

    # init content manager to simulate running "jupyter notebook"
    cm = PloomberContentsManager()

    # this must have an injected cell
    model = cm.get("plot.py")
    assert get_injected_cell(model["content"])

    # replace upstream dependency with a task that does not exist
    path = Path("plot.py")
    original_code = path.read_text()
    new_code = original_code.replace('{"clean": None}', '{"no_task": None}')
    path.write_text(new_code)

    # no cell should be injected this time
    model = cm.get("plot.py")
    assert not get_injected_cell(model["content"])

    # fix it, must inject cell again
    path.write_text(original_code)
    model = cm.get("plot.py")
    assert get_injected_cell(model["content"])


def test_server_extension_is_initialized():
    app = NotebookApp()
    app.initialize()
    assert hasattr(app.contents_manager, "load_dag")


def test_ignores_tasks_whose_source_is_not_a_file(
    monkeypatch, capsys, tmp_directory, no_sys_modules_cache
):
    """
    Context: jupyter extension only applies to tasks whose source is a script,
    otherwise it will break, trying to get the source location. This test
    checks that a SQLUpload (whose source is a data file) task is ignored
    from the extension
    """
    monkeypatch.setattr(sys, "argv", ["jupyter"])
    spec = {
        "meta": {
            "extract_product": False,
            "extract_upstream": False,
            "product_default_class": {"SQLUpload": "SQLiteRelation"},
        },
        "clients": {"SQLUpload": "db.get_client", "SQLiteRelation": "db.get_client"},
        "tasks": [
            {
                "source": "some_file.csv",
                "name": "task",
                "class": "SQLUpload",
                "product": ["some_table", "table"],
            }
        ],
    }

    with open("pipeline.yaml", "w") as f:
        yaml.dump(spec, f)

    Path("db.py").write_text(
        """
from ploomber.clients import SQLAlchemyClient

def get_client():
    return SQLAlchemyClient('sqlite://')
"""
    )

    Path("file.py").touch()

    app = NotebookApp()
    app.initialize()
    app.contents_manager.get("file.py")

    out, err = capsys.readouterr()

    assert "Traceback" not in err


def test_dag_manager(backup_spec_with_functions, no_sys_modules_cache):
    dag = DAGSpec("pipeline.yaml").to_dag().render()
    m = JupyterDAGManager(dag)

    assert set(m) == {
        "my_tasks/raw/functions.py (functions)",
        "my_tasks/raw/functions.py (functions)/raw",
        "my_tasks/clean/functions.py (functions)",
        "my_tasks/clean/functions.py (functions)/clean",
    }

    # TODO: test other methods


def test_dag_manager_flat_structure(backup_spec_with_functions_flat):
    dag = DAGSpec("pipeline.yaml").to_dag().render()
    m = JupyterDAGManager(dag)

    assert set(m) == {
        "my_tasks_flat/raw.py (functions)",
        "my_tasks_flat/raw.py (functions)/raw",
        "my_tasks_flat/raw.py (functions)/raw2",
        "my_tasks_flat/clean.py (functions)",
        "my_tasks_flat/clean.py (functions)/clean",
    }

    assert "my_tasks_flat/raw.py (functions)/" in m
    assert "/my_tasks_flat/raw.py (functions)/" in m
    assert "/my_tasks_flat/raw.py (functions)/" in m

    # TODO: test other methods
    # TODO: test folders are not created


def test_dag_manager_root_folder(backup_simple):
    dag = DAGSpec("pipeline.yaml").to_dag().render()
    m = JupyterDAGManager(dag)
    # jupyter represents the root folder with the empty string '', make sure
    # that correctly retuns the appropriate models
    content = m.get_by_parent("")

    assert len(content) == 1
    assert content[0]["name"] == "tasks_simple.py (functions)"
    assert content[0]["type"] == "directory"


def test_jupyter_workflow_with_functions(
    backup_spec_with_functions, no_sys_modules_cache
):
    """
    Tests a typical workflow with a pieline where some tasks are functions
    """
    cm = PloomberContentsManager()

    def get_names(out):
        return {model["name"] for model in out["content"]}

    assert get_names(cm.get("")) == {"my_tasks", "pipeline.yaml"}
    assert get_names(cm.get("my_tasks")) == {"__init__.py", "clean", "raw"}

    # check new notebooks appear, which are generated from the function tasks
    assert get_names(cm.get("my_tasks/raw")) == {
        "__init__.py",
        "functions.py",
        "functions.py (functions)",
    }
    assert get_names(cm.get("my_tasks/clean")) == {
        "__init__.py",
        "functions.py",
        "functions.py (functions)",
        "util.py",
    }

    # get notebooks generated from task functions
    raw = cm.get("my_tasks/raw/functions.py (functions)/raw")
    clean = cm.get("my_tasks/clean/functions.py (functions)/clean")

    # add some new code
    cell = nbformat.versions[nbformat.current_nbformat].new_code_cell("1 + 1")
    raw["content"]["cells"].append(cell)
    clean["content"]["cells"].append(cell)

    # overwrite the original function
    cm.save(raw, path="my_tasks/raw/functions.py (functions)/raw")
    cm.save(clean, path="my_tasks/clean/functions.py (functions)/clean")

    # make sure source code was updated
    raw_source = (
        backup_spec_with_functions / "my_tasks" / "raw" / "functions.py"
    ).read_text()
    clean_source = (
        backup_spec_with_functions / "my_tasks" / "clean" / "functions.py"
    ).read_text()

    assert "1 + 1" in raw_source
    assert "1 + 1" in clean_source


def test_hot_reload_when_adding_function_task(
    backup_spec_with_functions_flat, no_sys_modules_cache
):
    # setup: configure jupyter settings and save spec
    with open("pipeline.yaml") as f:
        spec = yaml.safe_load(f)

    spec["meta"]["jupyter_functions_as_notebooks"] = True
    spec["meta"]["jupyter_hot_reload"] = True

    Path("pipeline.yaml").write_text(yaml.dump(spec))

    # initialize contents manager (simulates user starting "jupyter notebook")
    cm = PloomberContentsManager()

    # user adds a new task...
    Path("new_task.py").write_text(
        """
def new_task(product):
    pass
"""
    )

    spec["tasks"].append({"source": "new_task.new_task", "product": "file.csv"})

    Path("pipeline.yaml").write_text(yaml.dump(spec))

    # content manager should now display the function
    assert "new_task.py (functions)" in [c["name"] for c in cm.get("")["content"]]

    assert ["new_task"] == [
        c["name"] for c in cm.get("new_task.py (functions)")["content"]
    ]


@pytest.mark.xfail
def test_hot_reload_when_adding_function_task_in_existing_module(
    backup_spec_with_functions_flat, no_sys_modules_cache
):
    # setup: configure jupyter settings and save spec
    with open("pipeline.yaml") as f:
        spec = yaml.safe_load(f)

    spec["meta"]["jupyter_functions_as_notebooks"] = True
    spec["meta"]["jupyter_hot_reload"] = True

    Path("pipeline.yaml").write_text(yaml.dump(spec))

    # initialize contents manager (simulates user starting "jupyter notebook")
    cm = PloomberContentsManager()

    # user adds a new task in existing module...
    path = Path("my_tasks_flat", "raw.py")
    source = path.read_text()
    new = "def function_new(product):\n    pass\n"
    path.write_text(source + new)

    spec["tasks"].append(
        {"source": "my_tasks_flat.raw.function_new", "product": "file.csv"}
    )

    Path("pipeline.yaml").write_text(yaml.dump(spec))

    # content manager should now display the function
    assert "raw.py (functions)" in [
        c["name"] for c in cm.get("my_tasks_flat")["content"]
    ]


def test_disable_functions_as_notebooks(backup_spec_with_functions):
    """
    Tests a typical workflow with a pieline where some tasks are functions
    """
    with open("pipeline.yaml") as f:
        spec = yaml.safe_load(f)

    spec["meta"]["jupyter_functions_as_notebooks"] = False
    Path("pipeline.yaml").write_text(yaml.dump(spec))

    cm = PloomberContentsManager()

    def get_names(out):
        return {model["name"] for model in out["content"]}

    assert get_names(cm.get("")) == {"my_tasks", "pipeline.yaml"}
    assert get_names(cm.get("my_tasks")) == {"__init__.py", "clean", "raw"}

    # check new notebooks appear, which are generated from the function tasks
    assert get_names(cm.get("my_tasks/raw")) == {
        "__init__.py",
        "functions.py",
    }
    assert get_names(cm.get("my_tasks/clean")) == {
        "__init__.py",
        "functions.py",
        "util.py",
    }


def test_injected_cell_with_resources(tmp_directory):
    resource = Path("file.txt")
    resource.touch()

    Path("notebooks").mkdir()
    path = Path("notebooks", "nb.py")
    path.write_text(
        """
from pathlib import Path

# + tags=["parameters"]
upstream = None
resources_ = None

# +
Path(resources_['file']).read_text()
    """
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: notebooks/nb.py
      product: out.ipynb
      params:
        resources_:
          file: file.txt
"""
    )

    cm = PloomberContentsManager()

    model = cm.get("notebooks/nb.py")

    absolute = str(resource.resolve())

    if sys.platform == "win32":
        absolute = absolute.replace("\\", "\\\\")

    cell = get_injected_cell(model["content"])
    assert absolute in cell["source"]


def test_ignores_static_analysis_failure(tmp_nbs):
    Path("pipeline.yaml").write_text(
        """
tasks:
  - source: load.py
    product:
      nb: output/load.ipynb
      data: output/data.csv
    params:
      some_param: some_value
"""
    )

    cm = PloomberContentsManager()

    model = cm.get(str("load.py"))
    assert get_injected_cell(model["content"])


def test_does_not_initialize_clients(tmp_nbs, tmp_imports):
    # initializing cients is an expensive operation, we shouldn't do it
    # when loading the DAG in the Jupyter plugin

    Path("clients.py").write_text(
        """
def get():
    raise Exception
"""
    )

    Path("pipeline.yaml").write_text(
        """
clients:
    File: clients.get

tasks:
  - source: load.py
    product:
      nb: output/load.ipynb
      data: output/data.csv
    params:
      some_param: some_value
"""
    )

    cm = PloomberContentsManager()

    model = cm.get(str("load.py"))
    assert get_injected_cell(model["content"])


def test_caches_dag(tmp_nbs):
    cm = PloomberContentsManager()

    cm.get("load.py")
    dag_first = cm.dag
    cm.get("load.py")
    dag_second = cm.dag
    cm.get("clean.py")
    dag_third = cm.dag

    assert dag_first is dag_second
    assert dag_second is dag_third


def test_reloads_dag_if_spec_changes(tmp_nbs):
    cm = PloomberContentsManager()

    cm.get("load.py")
    dag_first = cm.dag

    path = Path("pipeline.yaml")
    new = path.read_text() + "\n\n# some new comment"
    path.write_text(new)

    cm.get("clean.py")
    dag_second = cm.dag

    assert dag_first is not dag_second


def test_reloads_dag_if_path_changes_due_to_env_var(tmp_nbs, monkeypatch):
    shutil.copy("pipeline.yaml", "pipeline.another.yaml")

    cm = PloomberContentsManager()

    cm.get("load.py")
    dag_first = cm.dag

    monkeypatch.setenv("ENTRY_POINT", "pipeline.another.yaml")

    cm.get("clean.py")
    dag_second = cm.dag

    assert dag_first is not dag_second


def test_reloads_dag_if_path_changes_due_to_setup_cfg(tmp_nbs):
    shutil.copy("pipeline.yaml", "pipeline.another.yaml")

    cm = PloomberContentsManager()

    cm.get("load.py")
    dag_first = cm.dag

    Path("setup.cfg").write_text(
        """
[ploomber]
entry-point = pipeline.another.yaml
"""
    )

    cm.get("clean.py")
    dag_second = cm.dag

    assert dag_first is not dag_second


def test_reloads_dag_if_upstream_changes(tmp_nbs, monkeypatch):
    monkeypatch.setenv("ENTRY_POINT", "pipeline.extract-upstream.yaml")

    cm = PloomberContentsManager()

    cm.get("load.py")
    dag_first = cm.dag

    # change upstream
    nb = jupytext.read("plot.py")
    nb["cells"][1]["source"] = 'upstream = ["load"]'
    jupytext.write(nb, "plot.py")

    # request the one whose upstream changed
    cm.get("plot.py")
    dag_second = cm.dag

    assert dag_first is not dag_second


def test_caches_dag_if_upstream_changes_but_extra_upstream_is_false(tmp_nbs):
    cm = PloomberContentsManager()

    cm.get("load.py")
    dag_first = cm.dag

    # change upstream
    nb = jupytext.read("plot.py")
    nb["cells"][1]["source"] = 'upstream = ["load"]'
    jupytext.write(nb, "plot.py")

    # request the one whose upstream changed
    cm.get("plot.py")
    dag_second = cm.dag

    assert dag_first is dag_second


def test_reloads_dag_if_env_changes(tmp_nbs):
    yaml_ = YAML("pipeline.yaml")
    yaml_.data["tasks"][0]["params"] = dict(param="{{param}}")
    yaml_.write()

    yaml_env = YAML("env.yaml")
    yaml_env.data["param"] = "value"
    yaml_env.write()

    cm = PloomberContentsManager()
    cm.get("load.py")
    dag_first = cm.dag

    # change env
    yaml_env.data["param"] = "another-value"
    yaml_env.write()

    cm.get("load.py")
    dag_second = cm.dag

    assert dag_first is not dag_second


def test_reloads_dag_if_env_path_changes(tmp_nbs, monkeypatch):
    # parametrize pipeline
    yaml_ = YAML("pipeline.yaml")
    yaml_.data["tasks"][0]["params"] = dict(param="{{param}}")
    yaml_.write()

    # create two sample envs
    yaml_env = YAML("env.yaml")
    yaml_env.data["param"] = "value"
    yaml_env.write()
    shutil.copy("env.yaml", "env.another.yaml")

    cm = PloomberContentsManager()
    cm.get("load.py")
    dag_first = cm.dag

    # change env
    monkeypatch.setenv("PLOOMBER_ENV_FILENAME", "env.another.yaml")

    cm.get("load.py")
    dag_second = cm.dag

    assert dag_first is not dag_second


def test_load_dag_exception_handling(monkeypatch):
    cm = PloomberContentsManager()
    mock_exception = Mock(side_effect=ValueError("known error"))
    monkeypatch.setattr(cm.log, "exception", Mock())
    monkeypatch.setattr(cm, "_load_dag", mock_exception)

    cm.load_dag()

    msg = "An error occured when loading your pipeline: known error"
    assert cm.log.exception.call_args_list == [call(msg)]
