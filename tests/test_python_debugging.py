import shutil
import pytest
from pathlib import Path
import importlib
import tempfile

import test_pkg
from test_pkg import functions
import nbformat

from ploomber.sources.debugging import CallableDebugger
from ploomber.sources import debugging


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

    shutil.copytree(root, Path(backup, 'test_pkg'))

    yield

    shutil.rmtree(root)
    shutil.copytree(Path(backup, 'test_pkg'), root)
    shutil.rmtree(backup)


def replace_cell(nb, source, replacement):
    for cell in nb.cells:
        if cell['cell_type'] == 'code':
            if cell['source'] == source:
                cell['source'] = replacement


@pytest.mark.parametrize('fn,start',
                         [
                             (functions.simple, 1),
                             (functions.multiple_lines_signature, 3)
                         ])
def test_find_body_start_line(fn, start):
    assert debugging.parse(fn)[1] == start


@pytest.mark.parametrize('fn_name', ['simple',
                                     'multiple_lines_signature'])
def test_editing_function(fn_name, tmp_file, backup_test_pkg):

    with CallableDebugger(getattr(functions, fn_name),
                          {'upstream': None, 'product': None}) as tmp_nb:

        nb = nbformat.read(tmp_nb, as_version=nbformat.NO_CONVERT)
        replace_cell(nb, 'x = 1\n', 'x = 2\n')
        nbformat.write(nb, tmp_nb)

    reloaded = importlib.reload(functions)
    getattr(reloaded, fn_name)(None, None, tmp_file)
    print(Path(functions.__file__).read_text())
    assert Path(tmp_file).read_text() == '2'
