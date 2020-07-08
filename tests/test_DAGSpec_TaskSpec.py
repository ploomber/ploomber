from pathlib import Path
import pytest
from ploomber.spec.TaskSpec import TaskSpec
from ploomber.spec.DAGSpec import DAGSpec
from ploomber import DAG


@pytest.mark.parametrize('key', ['source', 'product'])
def test_validate_missing_source(key):
    with pytest.raises(KeyError):
        TaskSpec({key: None},
                 {'extract_product': False, 'extract_upstream': False})


@pytest.mark.parametrize('task, meta', [
    ({'upstream': 'some_task', 'product': None, 'source': None},
        {'extract_upstream': True, 'extract_product': False}),
    ({'product': 'report.ipynb', 'source': None}, {
     'extract_product': True, 'extract_upstream': False}),
])
def test_error_if_extract_but_keys_declared(task, meta):
    with pytest.raises(ValueError):
        TaskSpec(task, meta)


def test_add_hook(tmp_directory, add_current_to_sys_path):
    task = {'product': 'notebook.ipynb', 'source': 'source.py',
            'on_finish': 'hooks.some_hook', 'on_render': 'hooks.some_hook',
            'on_failure': 'hooks.some_hook'}
    meta = DAGSpec.default_meta()

    Path('source.py').touch()

    Path('hooks.py').write_text("""

def some_hook():
    pass
    """)

    dag = DAG()
    t, _ = TaskSpec(task, meta).to_task(dag)
    assert t.on_finish
    assert t.on_render
    assert t.on_failure
