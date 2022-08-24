"""
Tests for ScriptRunner
"""
from pathlib import Path

import jupytext
import nbformat
import pytest

from ploomber import DAG
from ploomber.exceptions import DAGBuildError
from ploomber.products import File
from ploomber.sources.nb_utils import find_cell_with_tag
from ploomber.tasks import ScriptRunner
from ploomber.tasks.notebook import NotebookMixin


@pytest.fixture
def tmp_env(tmp_directory):
    Path('script.py').write_text("""
from pathlib import Path

# %% tags=["parameters"]
upstream = None

# %%
Path(product).touch()
""")

    Path('script.R').write_text("""
# %% tags=["parameters"]
upstream <- NULL

# %%
file.create(product)
""")

    Path('script-with-magics.py').write_text("""
from pathlib import Path

# %% tags=["parameters"]
upstream = None

# %%
Path(product).touch()

# %%
%%time
x = 1
""")

    Path('script-with-exception.py').write_text("""
from pathlib import Path

# %% tags=["parameters"]
upstream = None

# %%
Path(product).touch()

# %%
raise ValueError("error")
""")


def test_script_runner_is_notebook_mixin():
    assert issubclass(ScriptRunner, NotebookMixin)


@pytest.mark.parametrize('script', [
    'script.py',
    'script.R',
])
def test_execute_script(tmp_env, script):
    dag = DAG()

    ScriptRunner(Path(script), File('output.txt'), dag=dag)

    dag.build()


def test_execute_script_with_magics(tmp_env):
    dag = DAG()

    ScriptRunner(Path('script-with-magics.py'), File('output.txt'), dag=dag)

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    assert 'IPython magics are not supported' in str(excinfo.value)


def test_execute_script_with_exception(tmp_env):
    dag = DAG()

    ScriptRunner(Path('script-with-exception.py'), File('output.txt'), dag=dag)

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    assert 'IPython magics are not supported' not in str(excinfo.value)


def test_execute_script_with_unknown_langguage(tmp_env):
    dag = DAG()

    ScriptRunner(Path('script.py'), File('output.txt'), dag=dag)
    dag['script'].source._language = 'some-lang'

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    assert 'ScriptRunner only works with Python and R scripts' in str(
        excinfo.value)


@pytest.mark.parametrize(
    'code, name, expected_fmt', [
        ['', 'file.py', 'light'],
        ['', 'file.py', 'light'],
        ['', 'file.py', 'light'],
        [
            nbformat.writes(nbformat.v4.new_notebook()),
            'file.ipynb',
            'ipynb',
        ],
        [
            nbformat.writes(nbformat.v4.new_notebook()),
            'file.ipynb',
            'ipynb',
        ],
        [
            '# %%\n1+1\n\n# %%\n2+2',
            'file.py',
            'percent',
        ],
    ])
def test_execute_script_add_parameters_cell_if_missing(tmp_directory, code,
                                                       name, expected_fmt,
                                                       capsys):
    path = Path(name)
    path.write_text(code)
    nb_old = jupytext.read(path)

    ScriptRunner(Path(name),
                 File('out.html'),
                 dag=DAG())

    # displays adding cell message
    captured = capsys.readouterr()
    assert ("missing the parameters cell, adding it at the top of the file"
            in captured.out)

    # checks parameters cell is added
    nb_new = jupytext.read(path)
    cell, idx = find_cell_with_tag(nb_new, 'parameters')

    # adds the cell at the top
    assert idx == 0

    # only adds one cell
    assert len(nb_old.cells) + 1 == len(nb_new.cells)

    # expected source content
    assert "add default values for parameters" in cell['source']

    # keeps the same format
    fmt, _ = (('ipynb', None) if path.suffix == '.ipynb' else
              jupytext.guess_format(path.read_text(), ext=path.suffix))
    assert fmt == expected_fmt
