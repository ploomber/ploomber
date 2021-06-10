"""
Tests for the custom jupyter contents manager
"""
import sys
import os
from pathlib import Path

import yaml
from ipython_genutils.tempdir import TemporaryDirectory
from notebook.services.contents.tests.test_manager import TestContentsManager
from notebook.notebookapp import NotebookApp
import jupytext
import parso
import nbformat

from ploomber.jupyter.manager import PloomberContentsManager
from ploomber.jupyter.dag import JupyterDAGManager
from ploomber.spec import DAGSpec


class PloomberContentsManagerTestCase(TestContentsManager):
    """
    This runs the original test suite from jupyter to make sure our
    content manager works ok

    https://github.com/jupyter/notebook/blob/b152dd314decda6edbaee1756bb6f6fc50c50f9f/notebook/services/contents/tests/test_manager.py#L218

    Docs: https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html#testing
    """ # noqa

    def setUp(self):
        self._temp_dir = TemporaryDirectory()
        self.td = self._temp_dir.name
        self.contents_manager = PloomberContentsManager(root_dir=self.td)


# the following tests check our custom logic


def get_injected_cell(nb):
    injected = None

    for cell in nb['cells']:
        if 'injected-parameters' in cell['metadata'].get('tags', []):
            injected = cell

    return injected


def test_injects_cell_if_file_in_dag(tmp_nbs):
    def resolve(path):
        return str(Path('.').resolve() / path)

    cm = PloomberContentsManager()
    model = cm.get('plot.py')

    injected = get_injected_cell(model['content'])

    assert injected

    upstream_expected = {
        "clean": {
            "nb": resolve("output/clean.ipynb"),
            "data": resolve("output/clean.csv")
        }
    }
    product_expected = resolve("output/plot.ipynb")

    upstream = None
    product = None

    for node in parso.parse(injected['source']).children:
        code = node.get_code()
        if 'upstream' in code:
            upstream = code.split('=')[1]
        elif 'product' in code:
            product = code.split('=')[1]

    assert upstream_expected == eval(upstream)
    assert product_expected == eval(product)


def test_injects_cell_even_if_pipeline_yaml_in_subdirectory(tmp_nbs):
    os.chdir('..')
    cm = PloomberContentsManager()
    model = cm.get(str('content/plot.py'))
    injected = get_injected_cell(model['content'])
    assert injected


def test_dag_from_directory(monkeypatch, tmp_nbs):
    # remove files we don't need for this test case
    Path('pipeline.yaml').unlink()
    Path('factory.py').unlink()

    monkeypatch.setenv('ENTRY_POINT', '.')

    cm = PloomberContentsManager()
    model = cm.get('plot.py')
    injected = get_injected_cell(model['content'])
    assert injected


def test_save(tmp_nbs):
    cm = PloomberContentsManager()
    model = cm.get('plot.py')

    # I found a bug when saving a .py file in jupyter notebook: the model
    # received by .save does not have a path, could not reproduce this issue
    # when running this test so I'm deleting it on purpose to simulate that
    # behavior - not sure why this is happening
    del model['path']

    source = model['content']['cells'][0]['source']
    model['content']['cells'][0]['source'] = '# modification\n' + source
    cm.save(model, path='/plot.py')

    nb = jupytext.read('plot.py')
    code = Path('plot.py').read_text()
    assert get_injected_cell(nb) is None
    assert '# modification' in code


def test_deletes_metadata_on_save(tmp_nbs):
    Path('output').mkdir()
    metadata = Path('output/.plot.ipynb.metadata')
    metadata.touch()

    cm = PloomberContentsManager()
    model = cm.get('plot.py')
    cm.save(model, path='/plot.py')

    assert not metadata.exists()


def test_deletes_metadata_on_save_for_file_used_multiple_times(tmp_directory):
    Path('my-task.py').write_text("""
# + tags=['parameters']
upstream = None
""")

    # generate two tasks with the same script (but different params)
    spec = {
        'tasks': [{
            'source': 'my-task.py',
            'name': 'my-task-',
            'product': 'my-task.ipynb',
            'grid': {
                'param': [1, 2]
            }
        }]
    }

    Path('pipeline.yaml').write_text(yaml.dump(spec))

    m1 = Path('.my-task-0.ipynb.metadata')
    m2 = Path('.my-task-1.ipynb.metadata')
    m1.touch()
    m2.touch()

    cm = PloomberContentsManager()
    model = cm.get('my-task.py')
    cm.save(model, path='/my-task.py')

    assert not m1.exists()
    assert not m2.exists()


def test_skips_if_file_not_in_dag(tmp_nbs):
    cm = PloomberContentsManager()
    model = cm.get('dummy.py')
    nb = jupytext.read('dummy.py')

    # this file is not part of the pipeline, the contents manager should not
    # inject cells
    assert len(model['content']['cells']) == len(nb.cells)


def test_import(tmp_nbs):
    # make sure we are able to import modules in the current working
    # directory
    Path('pipeline.yaml').unlink()
    os.rename('pipeline-w-location.yaml', 'pipeline.yaml')
    PloomberContentsManager()


def test_injects_cell_when_initialized_from_sub_directory(tmp_nbs_nested):
    # simulate initializing from a directory where we have to recursively
    # look for pipeline.yaml
    os.chdir('load')

    cm = PloomberContentsManager()
    model = cm.get('load.py')

    injected = get_injected_cell(model['content'])
    assert injected


def test_hot_reload(tmp_nbs):
    # modify base pipeline.yaml to enable hot reload
    with open('pipeline.yaml') as f:
        spec = yaml.load(f, Loader=yaml.SafeLoader)

    spec['meta']['jupyter_hot_reload'] = True
    spec['meta']['extract_upstream'] = True

    for t in spec['tasks']:
        t.pop('upstream', None)

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(spec, f)

    # init content manager to simulate running "jupyter notebook"
    cm = PloomberContentsManager()

    # this must have an injected cell
    model = cm.get('plot.py')
    assert get_injected_cell(model['content'])

    # replace upstream dependency with a task that does not exist
    path = Path('plot.py')
    original_code = path.read_text()
    new_code = original_code.replace("{'clean': None}", "{'no_task': None}")
    path.write_text(new_code)

    # no cell should be injected this time
    model = cm.get('plot.py')
    assert not get_injected_cell(model['content'])

    # fix it, must inject cell again
    path.write_text(original_code)
    model = cm.get('plot.py')
    assert get_injected_cell(model['content'])


def test_server_extension_is_initialized():
    app = NotebookApp()
    app.initialize()
    assert isinstance(app.contents_manager, PloomberContentsManager)


def test_ignores_tasks_whose_source_is_not_a_file(monkeypatch, capsys,
                                                  tmp_directory,
                                                  no_sys_modules_cache):
    """
    Context: jupyter extension only applies to tasks whose source is a script,
    otherwise it will break, trying to get the source location. This test
    checks that a SQLUpload (whose source is a data file) task is ignored
    from the extension
    """
    monkeypatch.setattr(sys, 'argv', ['jupyter'])
    spec = {
        'meta': {
            'extract_product': False,
            'extract_upstream': False,
            'product_default_class': {
                'SQLUpload': 'SQLiteRelation'
            }
        },
        'clients': {
            'SQLUpload': 'db.get_client',
            'SQLiteRelation': 'db.get_client'
        },
        'tasks': [{
            'source': 'some_file.csv',
            'name': 'task',
            'class': 'SQLUpload',
            'product': ['some_table', 'table']
        }]
    }

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(spec, f)

    Path('db.py').write_text("""
from ploomber.clients import SQLAlchemyClient

def get_client():
    return SQLAlchemyClient('sqlite://')
""")

    Path('file.py').touch()

    app = NotebookApp()
    app.initialize()
    app.contents_manager.get('file.py')

    out, err = capsys.readouterr()

    assert 'Traceback' not in err


def test_dag_manager(backup_spec_with_functions):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    m = JupyterDAGManager(dag)

    assert set(m) == {
        'my_tasks/raw/functions.py (functions)',
        'my_tasks/raw/functions.py (functions)/raw',
        'my_tasks/clean/functions.py (functions)',
        'my_tasks/clean/functions.py (functions)/clean'
    }

    # TODO: test other methods


def test_dag_manager_flat_structure(backup_spec_with_functions_flat):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    m = JupyterDAGManager(dag)

    assert set(m) == {
        'my_tasks_flat/raw.py (functions)',
        'my_tasks_flat/raw.py (functions)/raw',
        'my_tasks_flat/raw.py (functions)/raw2',
        'my_tasks_flat/clean.py (functions)',
        'my_tasks_flat/clean.py (functions)/clean',
    }

    assert 'my_tasks_flat/raw.py (functions)/' in m
    assert '/my_tasks_flat/raw.py (functions)/' in m
    assert '/my_tasks_flat/raw.py (functions)/' in m

    # TODO: test other methods
    # TODO: test folders are not created


def test_dag_manager_root_folder(backup_simple):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    m = JupyterDAGManager(dag)
    # jupyter represents the root folder with the empty string '', make sure
    # that correctly retuns the appropriate models
    content = m.get_by_parent('')

    assert len(content) == 1
    assert content[0]['name'] == 'tasks_simple.py (functions)'
    assert content[0]['type'] == 'directory'


def test_jupyter_workflow_with_functions(backup_spec_with_functions):
    """
    Tests a typical workflow with a pieline where some tasks are functions
    """
    cm = PloomberContentsManager()

    def get_names(out):
        return {model['name'] for model in out['content']}

    assert get_names(cm.get('')) == {'my_tasks', 'pipeline.yaml'}
    assert get_names(cm.get('my_tasks')) == {'__init__.py', 'clean', 'raw'}

    # check new notebooks appear, which are generated from the function tasks
    assert get_names(cm.get('my_tasks/raw')) == {
        '__init__.py',
        'functions.py',
        'functions.py (functions)',
    }
    assert get_names(cm.get('my_tasks/clean')) == {
        '__init__.py',
        'functions.py',
        'functions.py (functions)',
        'util.py',
    }

    # get notebooks generated from task functions
    raw = cm.get('my_tasks/raw/functions.py (functions)/raw')
    clean = cm.get('my_tasks/clean/functions.py (functions)/clean')

    # add some new code
    cell = nbformat.versions[nbformat.current_nbformat].new_code_cell('1 + 1')
    raw['content']['cells'].append(cell)
    clean['content']['cells'].append(cell)

    # overwrite the original function
    cm.save(raw, path='my_tasks/raw/functions.py (functions)/raw')
    cm.save(clean, path='my_tasks/clean/functions.py (functions)/clean')

    # make sure source code was updated
    raw_source = (backup_spec_with_functions / 'my_tasks' / 'raw' /
                  'functions.py').read_text()
    clean_source = (backup_spec_with_functions / 'my_tasks' / 'clean' /
                    'functions.py').read_text()

    assert '1 + 1' in raw_source
    assert '1 + 1' in clean_source


def test_hot_reload_when_adding_function_task(backup_spec_with_functions_flat):
    # setup: configure jupyter settings and save spec
    with open('pipeline.yaml') as f:
        spec = yaml.safe_load(f)

    spec['meta']['jupyter_functions_as_notebooks'] = True
    spec['meta']['jupyter_hot_reload'] = True

    Path('pipeline.yaml').write_text(yaml.dump(spec))

    # initialize contents manager (simulates user starting "jupyter notebook")
    cm = PloomberContentsManager()

    # user adds a new task...
    Path('new_task.py').write_text("""
def new_task(product):
    pass
""")

    spec['tasks'].append({
        'source': 'new_task.new_task',
        'product': 'file.csv'
    })

    Path('pipeline.yaml').write_text(yaml.dump(spec))

    # content manager should now display the function
    assert 'new_task.py (functions)' in [
        c['name'] for c in cm.get('')['content']
    ]

    assert ['new_task'] == [
        c['name'] for c in cm.get('new_task.py (functions)')['content']
    ]


def test_hot_reload_when_adding_function_task_in_existing_module(
        backup_spec_with_functions_flat):
    # setup: configure jupyter settings and save spec
    with open('pipeline.yaml') as f:
        spec = yaml.safe_load(f)

    spec['meta']['jupyter_functions_as_notebooks'] = True
    spec['meta']['jupyter_hot_reload'] = True

    Path('pipeline.yaml').write_text(yaml.dump(spec))

    # initialize contents manager (simulates user starting "jupyter notebook")
    cm = PloomberContentsManager()

    # user adds a new task in existing module...
    path = Path('my_tasks_flat', 'raw.py')
    source = path.read_text()
    new = 'def function_new(product):\n    pass\n'
    path.write_text(source + new)

    spec['tasks'].append({
        'source': 'my_tasks_flat.raw.function_new',
        'product': 'file.csv'
    })

    Path('pipeline.yaml').write_text(yaml.dump(spec))

    # content manager should now display the function
    assert 'raw.py (functions)' in [
        c['name'] for c in cm.get('my_tasks_flat')['content']
    ]


def test_disable_functions_as_notebooks(backup_spec_with_functions):
    """
    Tests a typical workflow with a pieline where some tasks are functions
    """
    with open('pipeline.yaml') as f:
        spec = yaml.safe_load(f)

    spec['meta']['jupyter_functions_as_notebooks'] = False
    Path('pipeline.yaml').write_text(yaml.dump(spec))

    cm = PloomberContentsManager()

    def get_names(out):
        return {model['name'] for model in out['content']}

    assert get_names(cm.get('')) == {'my_tasks', 'pipeline.yaml'}
    assert get_names(cm.get('my_tasks')) == {'__init__.py', 'clean', 'raw'}

    # check new notebooks appear, which are generated from the function tasks
    assert get_names(cm.get('my_tasks/raw')) == {
        '__init__.py',
        'functions.py',
    }
    assert get_names(cm.get('my_tasks/clean')) == {
        '__init__.py',
        'functions.py',
        'util.py',
    }
