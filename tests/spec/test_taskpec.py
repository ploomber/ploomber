from pathlib import Path

import pytest

from ploomber.spec.taskspec import TaskSpec, task_class_from_source_str
from ploomber.spec.dagspec import Meta
from ploomber.tasks import (NotebookRunner, SQLScript, SQLDump, ShellScript,
                            PythonCallable)
from ploomber.exceptions import DAGSpecInitializationError
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
        source_str, lazy_import=False, reload=False, product=None) is expected


def test_task_class_from_dotted_path(tmp_directory, add_current_to_sys_path):
    Path('test_task_class_from_dotted_path.py').write_text("""
def fn():
    pass
""")
    dotted_path = 'test_task_class_from_dotted_path.fn'
    assert task_class_from_source_str(
        dotted_path, lazy_import=False, reload=False,
        product=None) is PythonCallable


def test_task_class_from_source_str_error():
    with pytest.raises(ValueError):
        task_class_from_source_str('not_a_module.not_a_function',
                                   lazy_import=False,
                                   reload=False,
                                   product=None)


@pytest.mark.parametrize(
    'spec, expected',
    [
        [
            {
                'source': 'sample.py',
                'product': 'out.ipynb'
            },
            NotebookRunner,
        ],
        [
            {
                'source': 'sample.sql',
                'product': ['schema', 'table'],
                'client': 'db.get_client'
            },
            SQLScript,
        ],
        [
            {
                'source': 'sample-select.sql',
                'product': 'file.csv',
                'class': 'SQLDump',
                'client': 'db.get_client'
            },
            SQLDump,
        ],
        # special case: class is not present but SQLDump should be inferred
        # based on the product's extension
        [
            {
                'source': 'sample-select.sql',
                'product': 'file.csv',
                'client': 'db.get_client'
            },
            SQLDump,
        ],
        # same but with parquet
        [
            {
                'source': 'sample-select.sql',
                'product': 'file.parquet',
                'client': 'db.get_client'
            },
            SQLDump,
        ],
        # using a spec
        [
            {
                'source': 'sample-select.sql',
                'product': 'file.parquet',
                'client': {
                    'dotted_path': 'db.get_client'
                }
            },
            SQLDump,
        ],
        # using a spec with keyword args
        [
            {
                'source': 'sample-select.sql',
                'product': 'file.parquet',
                'client': {
                    'dotted_path': 'db.get_client',
                    'a': 1
                }
            },
            SQLDump,
        ],
        # product_client str
        [
            {
                'source': 'sample-select.sql',
                'product': 'file.parquet',
                'client': 'db.get_client',
                'product_client': 'db.get_client'
            },
            SQLDump,
        ],
        # product_client dict
        [
            {
                'source': 'sample-select.sql',
                'product': 'file.parquet',
                'client': 'db.get_client',
                'product_client': {
                    'dotted_path': 'db.get_client',
                    'a': 1
                }
            },
            SQLDump,
        ],
    ])
def test_initialization(spec, expected, tmp_sample_tasks,
                        add_current_to_sys_path, no_sys_modules_cache):
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


def test_error_on_invalid_value_for_file_product(backup_online,
                                                 add_current_to_sys_path):
    meta = Meta.default_meta()
    meta['extract_product'] = False

    spec = TaskSpec({
        'source': 'online_tasks.square',
        'product': 1,
    },
                    meta=meta,
                    project_root='.')

    with pytest.raises(TypeError) as excinfo:
        spec.to_task(dag=DAG())

    expected = ('Error initializing File with argument 1 '
                '(expected str, bytes or os.PathLike object, not int)')
    assert expected == str(excinfo.value)


@pytest.mark.parametrize('spec', [
    {
        'source': 'sample.sql',
        'product': 'some_invalid_product',
        'client': 'db.get_client'
    },
    {
        'source': 'sample.sql',
        'product': {
            'one': ['name', 'table'],
            'another': 'some_invalid_product'
        },
        'client': 'db.get_client'
    },
])
def test_error_when_failing_to_init(spec, tmp_sample_tasks,
                                    add_current_to_sys_path):
    meta = Meta.default_meta({
        'extract_product': False,
        'extract_upstream': True
    })

    dag = DAG()

    with pytest.raises(DAGSpecInitializationError) as excinfo:
        TaskSpec(spec, meta=meta, project_root='.').to_task(dag=dag)

    assert 'Error initializing SQLRelation' in str(excinfo.value)


def test_skips_source_loader_if_absolute_path(tmp_sample_tasks,
                                              add_current_to_sys_path):
    Path('templates').mkdir()

    meta = Meta.default_meta({
        'extract_product': False,
        'extract_upstream': True,
        'source_loader': {
            'path': 'templates'
        }
    })

    dag = DAG()

    spec = {
        'source': str(Path(tmp_sample_tasks, 'sample.sql')),
        'product': ['name', 'table'],
        'client': 'db.get_client'
    }

    assert TaskSpec(spec, meta=meta, project_root='.').to_task(dag=dag)


@pytest.mark.parametrize('key', ['client', 'product_client'])
def test_error_if_client_dotted_path_returns_none(tmp_sample_tasks,
                                                  add_current_to_sys_path,
                                                  no_sys_modules_cache, key):
    Path('client_dotted_path_returns_none.py').write_text("""
def get():
    return None
""")

    meta = Meta.default_meta({
        'extract_product': False,
        'extract_upstream': True,
    })

    dag = DAG()

    spec = {
        'source': 'sample.sql',
        'product': ['name', 'table'],
    }

    spec[key] = 'client_dotted_path_returns_none.get'

    with pytest.raises(TypeError) as excinfo:
        TaskSpec(spec, meta=meta, project_root='.').to_task(dag=dag)

    assert (
        "Error calling dotted path "
        "'client_dotted_path_returns_none.get'. Expected a value but got None"
    ) in str(excinfo.value)


@pytest.mark.parametrize('key', [
    'on_finish',
    'on_failure',
    'on_render',
    'serializer',
    'unserializer',
    'client',
    'product_client',
])
def test_error_if_dotted_path_does_not_return_a_callable(
        backup_spec_with_functions_flat, add_current_to_sys_path,
        no_sys_modules_cache, key):

    Path('test_error_if_dotted_path_does_not_return_a_callable.py').write_text(
        """
some_non_function = 1
""")

    meta = Meta.default_meta({'extract_product': False})

    spec = {
        'source': 'my_tasks_flat.raw.function',
        'product': 'some_file.txt',
    }

    spec[key] = ('test_error_if_dotted_path_does_not_return_a_callable'
                 '.some_non_function')

    with pytest.raises(TypeError) as excinfo:
        TaskSpec(spec, meta=meta, project_root='.').to_task(dag=DAG())

    expected = ("Error loading dotted path 'test_error_if_dotted_path"
                "_does_not_return_a_callable.some_non_function'. Expected a "
                "callable object (i.e., some kind of function). Got "
                "1 (an object of type: int)")
    assert str(excinfo.value) == expected


def test_error_on_invalid_class(backup_spec_with_functions_flat,
                                add_current_to_sys_path):
    meta = Meta.default_meta({'extract_product': False})

    spec = {
        'source': 'my_tasks_flat.raw.function',
        'product': 'some_file.txt',
        'class': 'unknown_class'
    }

    with pytest.raises(ValueError) as excinfo:
        TaskSpec(spec, meta=meta, project_root='.').to_task(dag=DAG())

    expected = ("Error validating Task spec (class field): "
                "'unknown_class' is not a valid Task class name")
    assert str(excinfo.value) == expected


def test_error_on_invalid_product_class(backup_spec_with_functions_flat,
                                        add_current_to_sys_path):
    meta = Meta.default_meta({'extract_product': False})

    spec = {
        'source': 'my_tasks_flat.raw.function',
        'product': 'some_file.txt',
        'product_class': 'unknown_class'
    }

    with pytest.raises(ValueError) as excinfo:
        TaskSpec(spec, meta=meta, project_root='.').to_task(dag=DAG())

    expected = ("Error validating Task spec (product_class field): "
                "'unknown_class' is not a valid Product class name")
    assert str(excinfo.value) == expected


@pytest.fixture
def spec():
    return {
        'source': 'my_tasks_flat.raw.function',
        'name': 'function-',
        'product': 'some_file.txt',
        'grid': {
            'a': [1, 2],
            'b': [3, 4]
        }
    }


def test_grid(backup_spec_with_functions_flat, add_current_to_sys_path, spec):
    meta = Meta.default_meta()
    dag = DAG()

    task_group, _ = TaskSpec(spec, meta, project_root='.').to_task(dag=dag)

    assert len(task_group) == 4
    assert str(dag['function-0'].product) == str(
        Path('some_file-0.txt').resolve())
    assert str(dag['function-1'].product) == str(
        Path('some_file-1.txt').resolve())
    assert str(dag['function-2'].product) == str(
        Path('some_file-2.txt').resolve())
    assert str(dag['function-3'].product) == str(
        Path('some_file-3.txt').resolve())


def test_grid_with_missing_name(backup_spec_with_functions_flat,
                                add_current_to_sys_path, spec):
    del spec['name']

    with pytest.raises(KeyError) as excinfo:
        TaskSpec(spec, Meta.default_meta(),
                 project_root='.').to_task(dag=DAG())

    assert 'Error initializing task with spec' in str(excinfo.value)
