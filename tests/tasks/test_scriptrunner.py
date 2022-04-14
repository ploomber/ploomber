"""
Tests for ScriptRunner
"""
from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import ScriptRunner
from ploomber.tasks.notebook import NotebookMixin
from ploomber.products import File
from ploomber.exceptions import DAGBuildError


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
