import shutil
import pytest
from pathlib import Path
import importlib
import tempfile

import test_pkg
from test_pkg import functions
import nbformat

from ploomber.sources.debugging import CallableDebugger


@pytest.fixture
def tmp_file():
    _, tmp = tempfile.mkstemp()
    yield tmp
    Path(tmp).unlink()


@pytest.fixture
def backup_test_pkg():
    backup = tempfile.mkdtemp()
    root = Path(test_pkg.__file__).parents[2]

    # sanity check, in case we change the structure
    assert root.name == 'test_pkg'

    shutil.copytree(root.parent, Path(backup, 'test_pkg'))

    yield

    shutil.rmtree(root)
    shutil.copytree(Path(backup, 'test_pkg'), root)


def replace_cell(nb, source, replacement):
    for cell in nb.cells:
        if cell['cell_type'] == 'code':
            if cell['source'] == source:
                cell['source'] = replacement


@pytest.mark.parametrize('fn', [functions.simple,
                                functions.multiple_lines_signature])
def test_simple_case(fn, tmp_file):

    with CallableDebugger(fn,
                          {'upstream': None, 'product': None}) as tmp_nb:

        nb = nbformat.read(tmp_nb, as_version=nbformat.NO_CONVERT)
        replace_cell(nb, 'x = 1\n', 'x = 2\n')
        nbformat.write(nb, tmp_nb)

    importlib.reload(functions)
    fn(None, None, tmp_file)
    assert Path(tmp_file).read_text() == '2'
