import inspect
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


def replace_first_cell(nb, source, replacement):
    for cell in nb.cells:
        if cell['cell_type'] == 'code':
            if cell['source'] == source:
                cell['source'] = replacement
                return

    raise Exception('Cell with source "{}" not found'.format(source))


@pytest.mark.parametrize('fn,start',
                         [
                             (functions.simple, 0),
                             (functions.multiple_lines_signature, 2)
                         ])
def test_find_signature_last_line(fn, start):
    assert debugging.parse(fn)[1] == start


@pytest.mark.parametrize('fn_name', ['simple',
                                     'simple_w_docstring',
                                     'simple_w_docstring_long',
                                     'multiple_lines_signature'])
def test_editing_function(fn_name, tmp_file, backup_test_pkg):

    with CallableDebugger(getattr(functions, fn_name),
                          {'upstream': None, 'product': None}) as tmp_nb:

        nb = nbformat.read(tmp_nb, as_version=nbformat.NO_CONVERT)
        replace_first_cell(nb, 'x = 1', 'x = 2')
        nbformat.write(nb, tmp_nb)

    reloaded = importlib.reload(functions)
    getattr(reloaded, fn_name)(None, None, tmp_file)
    print(Path(functions.__file__).read_text())
    assert Path(tmp_file).read_text() == '2'

# TODO: test with docstring


@pytest.mark.parametrize('fn_name', ['simple',
                                     'simple_w_docstring',
                                     'simple_w_docstring_long',
                                     'multiple_lines_signature'])
def test_unmodified_function(fn_name, tmp_file, backup_test_pkg):
    print(Path(functions.__file__).read_text())
    source_original = inspect.getsource(getattr(functions, fn_name))

    with CallableDebugger(getattr(functions, fn_name),
                          {'upstream': None, 'product': None}):
        pass

    reloaded = importlib.reload(functions)
    source_new = inspect.getsource(getattr(reloaded, fn_name))
    print(Path(functions.__file__).read_text())
    assert source_original == source_new
