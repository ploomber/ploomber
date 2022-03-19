from pathlib import Path

import pytest

from ploomber.spec.taskspec import TaskSpec, task_class_from_source_str
from ploomber.spec.dagspec import Meta
from ploomber.tasks import (NotebookRunner, SQLScript, SQLDump, ShellScript,
                            PythonCallable)
from ploomber.exceptions import DAGSpecInitializationError, ValidationError
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
    assert task_class_from_source_str(
        source_str, lazy_import=False, reload=False, product=None) is expected


@pytest.mark.parametrize('source_str', [
    str(Path('something', 'script.md')),
    str(Path('something', 'another', 'script.json')),
])
def test_task_class_from_script_unknown_extension(tmp_directory, source_str):
    with pytest.raises(DAGSpecInitializationError) as excinfo:
        task_class_from_source_str(source_str,
                                   lazy_import=False,
                                   reload=False,
                                   product=None)

    assert 'Failed to determine task class' in str(excinfo.value)
    assert 'invalid extension' in str(excinfo.value)


def test_task_class_from_dotted_path(tmp_directory, tmp_imports):
    Path('test_task_class_from_dotted_path.py').write_text("""
def fn():
    pass
""")
    dotted_path = 'test_task_class_from_dotted_path.fn'
    assert task_class_from_source_str(
        dotted_path, lazy_import=False, reload=False,
        product=None) is PythonCallable


def test_task_class_from_source_str_error():
    with pytest.raises(DAGSpecInitializationError):
        task_class_from_source_str('not_a_module.not_a_function',
                                   lazy_import=False,
                                   reload=False,
                                   product=None)


def test_task_class_from_source_str_invalid_path():
    with pytest.raises(DAGSpecInitializationError) as excinfo:
        task_class_from_source_str([],
                                   lazy_import=None,
                                   reload=None,
                                   product=None)

    error = "Failed to initialize task from source []"
    assert error in str(excinfo.value)
    repr_ = str(excinfo.getrepr())
    assert 'expected str' in repr_


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
def test_initialization(spec, expected, tmp_sample_tasks, tmp_imports):
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
    with pytest.raises(ValidationError):
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
    with pytest.raises(DAGSpecInitializationError):
        TaskSpec(task, meta, project_root='.')


def test_add_hook(tmp_directory, tmp_imports):
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


def test_loads_serializer_and_unserializer(backup_online, tmp_imports):
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

    assert task._serializer.callable is serialize
    assert task._unserializer.callable is unserialize


def test_error_on_invalid_value_for_file_product(backup_online, tmp_imports):
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
def test_error_when_failing_to_init(spec, tmp_sample_tasks, tmp_imports):
    meta = Meta.default_meta({
        'extract_product': False,
        'extract_upstream': True
    })

    dag = DAG()

    with pytest.raises(DAGSpecInitializationError) as excinfo:
        TaskSpec(spec, meta=meta, project_root='.').to_task(dag=dag)

    assert 'Error initializing SQLRelation' in str(excinfo.value)


def test_skips_source_loader_if_absolute_path(tmp_sample_tasks, tmp_imports):
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
                                                  tmp_imports, key):
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
        backup_spec_with_functions_flat, tmp_imports, key):

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


def test_error_on_invalid_class(backup_spec_with_functions_flat, tmp_imports):
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
                                        tmp_imports):
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


def test_error_on_invalid_source(backup_spec_with_functions_flat, tmp_imports):
    meta = Meta.default_meta({'extract_product': False})

    spec = {'source': 'someinvalidstring', 'product': 'output_product'}

    with pytest.raises(DAGSpecInitializationError) as excinfo:
        TaskSpec(spec, meta=meta, project_root='.').to_task(dag=DAG())

    expected = ("Failed to determine task source 'someinvalidstring'\n"
                "Valid extensions are: '.R', '.Rmd', '.ipynb', '.py', '.r', "
                "'.sh', and '.sql'\nYou can also define functions as "
                "[module_name].[function_name]")
    assert str(excinfo.value) == expected


@pytest.fixture
def grid_spec():
    return {
        'source': 'my_tasks_flat.raw.function',
        'name': 'function-',
        'product': 'some_file.txt',
        'grid': {
            'a': [1, 2],
            'b': [3, 4]
        }
    }


def test_grid(backup_spec_with_functions_flat, tmp_imports, grid_spec):
    meta = Meta.default_meta()
    dag = DAG()

    task_group, _ = TaskSpec(grid_spec, meta,
                             project_root='.').to_task(dag=dag)

    assert len(task_group) == 4
    assert str(dag['function-0'].product) == str(
        Path('some_file-0.txt').resolve())
    assert str(dag['function-1'].product) == str(
        Path('some_file-1.txt').resolve())
    assert str(dag['function-2'].product) == str(
        Path('some_file-2.txt').resolve())
    assert str(dag['function-3'].product) == str(
        Path('some_file-3.txt').resolve())


def test_grid_with_hook(backup_spec_with_functions_flat, tmp_imports):
    grid_spec = {
        'source': 'my_tasks_flat.raw.function',
        'name': 'function-',
        'product': 'some_file.txt',
        'grid': {
            'a': [1, 2],
            'b': [3, 4]
        },
        'on_render': 'hooks.on_render',
        'on_finish': 'hooks.on_finish',
        'on_failure': 'hooks.on_failure',
    }

    meta = Meta.default_meta()
    dag = DAG()

    TaskSpec(grid_spec, meta, project_root='.').to_task(dag=dag)

    import hooks

    assert all(t.on_render.callable is hooks.on_render for t in dag.values())
    assert all(t.on_finish.callable is hooks.on_finish for t in dag.values())
    assert all(t.on_failure.callable is hooks.on_failure for t in dag.values())


def test_grid_with_hook_lazy_import(backup_spec_with_functions_flat,
                                    tmp_imports):
    grid_spec = {
        'source': 'my_tasks_flat.raw.function',
        'name': 'function-',
        'product': 'some_file.txt',
        'grid': {
            'a': [1, 2],
            'b': [3, 4]
        },
        'on_render': 'hooks.on_render',
        'on_finish': 'hooks.on_finish',
        'on_failure': 'hooks.on_failure',
    }

    meta = Meta.default_meta()
    dag = DAG()

    TaskSpec(grid_spec, meta, project_root='.',
             lazy_import=True).to_task(dag=dag)

    assert all(t.on_render.callable is None for t in dag.values())
    assert all(t.on_finish.callable is None for t in dag.values())
    assert all(t.on_failure.callable is None for t in dag.values())

    assert all(t.on_render._spec.dotted_path == 'hooks.on_render'
               for t in dag.values())
    assert all(t.on_finish._spec.dotted_path == 'hooks.on_finish'
               for t in dag.values())
    assert all(t.on_failure._spec.dotted_path == 'hooks.on_failure'
               for t in dag.values())


def test_grid_with_missing_name(backup_spec_with_functions_flat, tmp_imports,
                                grid_spec):
    del grid_spec['name']

    with pytest.raises(DAGSpecInitializationError) as excinfo:
        TaskSpec(grid_spec, Meta.default_meta(),
                 project_root='.').to_task(dag=DAG())

    assert 'Error initializing task with source' in str(excinfo.value)


def test_grid_and_params(backup_spec_with_functions_flat, tmp_imports,
                         grid_spec):
    grid_spec['params'] = {'a': 1}

    with pytest.raises(DAGSpecInitializationError) as excinfo:
        TaskSpec(grid_spec, Meta.default_meta(),
                 project_root='.').to_task(dag=DAG())

    assert "'params' is not allowed when using 'grid'" in str(excinfo.value)


# TODO: try with task clients
def test_lazy_load(tmp_directory, tmp_imports):
    Path('my_module.py').write_text("""
def fn():
    pass
""")

    meta = Meta.default_meta()
    spec = TaskSpec(
        {
            'source': 'my_module.fn',
            'product': 'report.ipynb',
            'on_finish': 'not_a_module.not_a_function',
            'on_render': 'not_a_module.not_a_function',
            'on_failure': 'not_a_module.not_a_function',
            'serializer': 'not_a_module.not_a_function',
            'unserializer': 'not_a_module.not_a_function',
        },
        meta,
        '.',
        lazy_import=True)

    assert spec.to_task(dag=DAG())


def test_constructor_deep_copies_spec_and_meta(tmp_directory, tmp_imports):
    prod_default_class = {'SQLScript': 'SQLRelation'}
    meta = Meta.default_meta({
        'extract_product': False,
        'product_default_class': prod_default_class
    })
    params = {'params': {'a': 1}}
    spec = {
        'source': 'sample.sql',
        'product': 'some_file.txt',
        'params': params
    }
    task_spec = TaskSpec(data=spec, meta=meta, project_root='.')

    assert spec is not task_spec.data
    assert meta is not task_spec.meta
    assert params is not task_spec.data['params']
    assert prod_default_class is not task_spec.meta['product_default_class']
