from pathlib import Path
import pytest
from ploomber.spec.TaskSpec import TaskSpec, task_class_from_source_str
from ploomber.spec.dagspec import Meta
from ploomber.tasks import (NotebookRunner, SQLScript, SQLDump, ShellScript,
                            PythonCallable)
from ploomber import DAG


@pytest.mark.parametrize('source_str, expected', [
    ['script.py', NotebookRunner],
    ['script.R', NotebookRunner],
    ['script.r', NotebookRunner],
    ['script.Rmd', NotebookRunner],
    ['script.ipynb', NotebookRunner],
    ['script.sql', SQLScript],
    ['script.sh', ShellScript],
])
def test_task_class_from_script(tmp_directory, source_str, expected):
    Path(source_str).touch()
    assert task_class_from_source_str(
        source_str, lazy_import=False, reload=False) is expected


def test_task_class_from_dotted_path(tmp_directory, add_current_to_sys_path):
    Path('test_task_class_from_dotted_path.py').write_text("""
def fn():
    pass
""")
    dotted_path = 'test_task_class_from_dotted_path.fn'
    assert task_class_from_source_str(
        dotted_path, lazy_import=False, reload=False) is PythonCallable


def test_task_class_from_source_str_error():
    with pytest.raises(ValueError):
        task_class_from_source_str('not_a_module.not_a_function',
                                   lazy_import=False,
                                   reload=False)


@pytest.mark.parametrize('spec, expected', [
    [{
        'source': 'sample.py',
        'product': 'out.ipynb'
    }, NotebookRunner],
    [{
        'source': 'sample.sql',
        'product': ['schema', 'table'],
        'client': 'db.get_client'
    }, SQLScript],
    [{
        'source': 'sample-select.sql',
        'product': 'file.csv',
        'class': 'SQLDump',
        'client': 'db.get_client'
    }, SQLDump],
])
def test_initialization(spec, expected, tmp_sample_tasks,
                        add_current_to_sys_path):
    meta = Meta.default_meta({
        'extract_product': False,
        'extract_upstream': True
    })

    spec = TaskSpec(spec, meta=meta, project_root='.')

    # check values after initialization
    assert spec['class'] == expected
    assert isinstance(spec['source'], Path)

    # check we can convert it to a Task
    spec.to_task(dag=DAG())


@pytest.mark.parametrize('key', ['source', 'product'])
def test_validate_missing_source(key):
    with pytest.raises(KeyError):
        TaskSpec({key: None}, {
            'extract_product': False,
            'extract_upstream': False
        },
                 project_root='.')


@pytest.mark.parametrize('task, meta', [
    ({
        'upstream': 'some_task',
        'product': None,
        'source': None
    }, {
        'extract_upstream': True,
        'extract_product': False
    }),
    ({
        'product': 'report.ipynb',
        'source': None
    }, {
        'extract_product': True,
        'extract_upstream': False
    }),
])
def test_error_if_extract_but_keys_declared(task, meta):
    with pytest.raises(ValueError):
        TaskSpec(task, meta, project_root='.')


def test_add_hook(tmp_directory, add_current_to_sys_path):
    task = {
        'product': 'notebook.ipynb',
        'source': 'source.py',
        'on_finish': 'hooks.some_hook',
        'on_render': 'hooks.some_hook',
        'on_failure': 'hooks.some_hook'
    }
    meta = Meta.default_meta()
    meta['extract_product'] = False

    Path('source.py').write_text("""
# + tags=["parameters"]
# some code
    """)

    Path('hooks.py').write_text("""

def some_hook():
    pass
    """)

    dag = DAG()
    t, _ = TaskSpec(task, meta, project_root='.').to_task(dag)
    assert t.on_finish
    assert t.on_render
    assert t.on_failure


def test_loads_serializer_and_unserializer(backup_online,
                                           add_current_to_sys_path):
    meta = Meta.default_meta()
    meta['extract_product'] = False

    spec = TaskSpec(
        {
            'source': 'online_tasks.square',
            'product': 'output/square.parquet',
            'serializer': 'online_io.serialize',
            'unserializer': 'online_io.unserialize',
        },
        meta=meta,
        project_root='.')

    dag = DAG()
    task, _ = spec.to_task(dag=dag)

    from online_io import serialize, unserialize

    assert task._serializer is serialize
    assert task._unserializer is unserialize
