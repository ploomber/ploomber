"""
Tests for the custom jupyter contents manager
"""
import os
from pathlib import Path

import yaml
from ipython_genutils.tempdir import TemporaryDirectory
from notebook.services.contents.tests.test_manager import TestContentsManager
import jupytext

from ploomber.jupyter import PloomberContentsManager


class PloomberContentsManagerTestCase(TestContentsManager):
    """
    This runs the original test suite from jupyter to make sure our
    content manager works ok

    https://github.com/jupyter/notebook/blob/b152dd314decda6edbaee1756bb6f6fc50c50f9f/notebook/services/contents/tests/test_manager.py#L218

    Docs: https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html#testing
    """

    def setUp(self):
        self._temp_dir = TemporaryDirectory()
        self.td = self._temp_dir.name
        self.contents_manager = PloomberContentsManager(root_dir=self.td)


# FIXME: test with nested notebooks
# the following tests check our custom logic

def get_injected_cell(nb):
    injected = None

    for cell in nb['cells']:
        if 'injected-parameters' in cell['metadata'].get('tags', []):
            injected = cell

    return injected


def test_injects_cell_if_file_in_dag(tmp_nbs):
    cm = PloomberContentsManager()
    model = cm.get('plot.py')

    injected = get_injected_cell(model['content'])

    assert injected
    upstream_expected = {"clean": {"nb": "output/clean.ipynb",
                                   "data": "output/clean.csv"}}
    product_expected = "output/plot.ipynb"

    upstream = None
    product = None

    # there is no order guarantee in Python 3.5, so we look for the definitions
    for line in injected['source'].split('\n'):
        if line.startswith('upstream'):
            upstream = line.split('=')[1]
        elif line.startswith('product'):
            product = line.split('=')[1]

    assert upstream_expected == eval(upstream)
    assert product_expected == eval(product)


def test_removes_injected_cell(tmp_nbs):
    cm = PloomberContentsManager()
    model = cm.get('plot.py')
    cm.save(model, path='/plot.py')

    nb = jupytext.read('plot.py')
    assert get_injected_cell(nb) is None


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

    cm = PloomberContentsManager()

    model = cm.get('plot.py')
    assert get_injected_cell(model['content'])

    # replace upstream with a task that does not exist
    path = Path('plot.py')
    original_code = path.read_text()
    new_code = original_code.replace("{'clean': None}", "{'no_task': None}")
    path.write_text(new_code)

    # not cell should be injected now
    model = cm.get('plot.py')
    assert not get_injected_cell(model['content'])

    # fix it
    path.write_text(original_code)
    model = cm.get('plot.py')
    assert get_injected_cell(model['content'])
