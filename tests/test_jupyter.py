"""
Tests for the custom jupyter contents manager
"""
import os
from pathlib import Path

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
    expected = ('# Parameters\nupstream = '
                '{"clean": {"nb": "output/clean.ipynb", "data": '
                '"output/clean.csv"}}\nproduct = '
                '"output/plot.ipynb"\n')
    assert expected == injected['source']


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
    # dicrectory
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
