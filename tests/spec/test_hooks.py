from pathlib import Path

import pytest

from ploomber.spec import DAGSpec
from ploomber.executors import Serial
from ploomber.exceptions import DAGBuildError


@pytest.fixture
def write_dag_hooks_spec():
    Path("my_module.py").write_text(
        """
from pathlib import Path

def touch(product):
    Path(product).touch()
"""
    )

    Path("hook.py").write_text(
        """
from pathlib import Path

def hook(param):
    Path('hook').write_text(param)
"""
    )

    Path("pipeline.yaml").write_text(
        """
on_finish:
  dotted_path: hook.hook
  param: 'on finish'
on_failure:
  dotted_path: hook.hook
  param: 'on failure'
on_render:
  dotted_path: hook.hook
  param: 'on render'

tasks:
    - source: my_module.touch
      product: out
"""
    )


@pytest.fixture
def write_task_hooks_spec():
    Path("my_module.py").write_text(
        """
from pathlib import Path

def touch(product):
    Path(product).touch()
"""
    )

    Path("hook.py").write_text(
        """
from pathlib import Path

def hook(param):
    Path('hook').write_text(param)
"""
    )

    Path("pipeline.yaml").write_text(
        """
tasks:
    - source: my_module.touch
      product: out
      on_finish:
        dotted_path: hook.hook
        param: 'on finish'
      on_failure:
        dotted_path: hook.hook
        param: 'on failure'
      on_render:
        dotted_path: hook.hook
        param: 'on render'
"""
    )


def test_dag_on_finish_with_params(tmp_directory, tmp_imports, write_dag_hooks_spec):
    dag = DAGSpec("pipeline.yaml").to_dag()
    dag.executor = Serial(build_in_subprocess=False)

    dag.build()

    assert Path("hook").read_text() == "on finish"


def test_dag_on_render_with_params(tmp_directory, tmp_imports, write_dag_hooks_spec):
    dag = DAGSpec("pipeline.yaml").to_dag()
    dag.executor = Serial(build_in_subprocess=False)

    dag.render()

    assert Path("hook").read_text() == "on render"


def test_dag_on_failure_with_params(tmp_directory, tmp_imports, write_dag_hooks_spec):
    Path("my_module.py").write_text(
        """
def touch(product):
    raise Exception
"""
    )

    dag = DAGSpec("pipeline.yaml").to_dag()
    dag.executor = Serial(build_in_subprocess=False)

    with pytest.raises(DAGBuildError):
        dag.build()

    assert Path("hook").read_text() == "on failure"


def test_task_on_finish_with_params(tmp_directory, tmp_imports, write_task_hooks_spec):
    dag = DAGSpec("pipeline.yaml").to_dag()
    dag.executor = Serial(build_in_subprocess=False)

    dag.build()

    assert Path("hook").read_text() == "on finish"


def test_task_on_render_with_params(tmp_directory, tmp_imports, write_task_hooks_spec):
    dag = DAGSpec("pipeline.yaml").to_dag()
    dag.executor = Serial(build_in_subprocess=False)

    dag.render()

    assert Path("hook").read_text() == "on render"


def test_task_on_failure_with_params(tmp_directory, tmp_imports, write_task_hooks_spec):
    Path("my_module.py").write_text(
        """
def touch(product):
    raise Exception
"""
    )

    dag = DAGSpec("pipeline.yaml").to_dag()
    dag.executor = Serial(build_in_subprocess=False)

    with pytest.raises(DAGBuildError):
        dag.build()

    assert Path("hook").read_text() == "on failure"
