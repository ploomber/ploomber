from unittest.mock import Mock
import inspect
import pytest
from pathlib import Path
import importlib
import tempfile

import parso
from test_pkg import functions
import nbformat

from ploomber.sources.interact import CallableInteractiveDeveloper
from ploomber.sources import interact


@pytest.fixture
def tmp_file():
    _, tmp = tempfile.mkstemp()
    yield tmp
    Path(tmp).unlink()


def replace_first_cell(nb, source, replacement):
    for cell in nb.cells:
        if cell['cell_type'] == 'code':
            if cell['source'] == source:
                cell['source'] = replacement
                return

    raise Exception('Cell with source "{}" not found'.format(source))


def find_cell_tagged(nb, tag):
    for cell in nb.cells:
        if tag in cell['metadata'].get('tags', {}):
            return cell


@pytest.mark.parametrize(
    'fn,start',
    [(functions.simple, 0),
     (functions.
      this_is_a_function_with_a_very_long_name_with_forces_us_to_split_params,
      1)])
def test_find_signature_last_line(fn, start):
    assert interact.parse_function(fn)[1] == start


@pytest.mark.parametrize('fn_name', [
    'simple', 'simple_w_docstring', 'simple_w_docstring_long',
    'multiple_lines_signature',
    'this_is_a_function_with_a_very_long_name_with_forces_us_to_split_params'
])
def test_editing_function(fn_name, tmp_file, backup_test_pkg):

    with CallableInteractiveDeveloper(getattr(functions, fn_name), {
            'upstream': None,
            'product': None
    }) as tmp_nb:

        nb = nbformat.read(tmp_nb, as_version=nbformat.NO_CONVERT)
        replace_first_cell(nb, 'x = 1', 'x = 2')
        nbformat.write(nb, tmp_nb)

    reloaded = importlib.reload(functions)
    getattr(reloaded, fn_name)(None, None, tmp_file)
    print(Path(functions.__file__).read_text())
    assert Path(tmp_file).read_text() == '2'
    assert not Path(tmp_nb).exists()


@pytest.mark.parametrize('fn_name', [
    'simple', 'simple_w_docstring', 'simple_w_docstring_long',
    'multiple_lines_signature',
    'this_is_a_function_with_a_very_long_name_with_forces_us_to_split_params'
])
@pytest.mark.parametrize('remove_trailing_newline', [False, True])
def test_unmodified_function(fn_name, remove_trailing_newline,
                             backup_test_pkg):
    """
    This test makes sure the file is not modified if we don't change the
    notebook because whitespace is tricky
    """
    fn = getattr(functions, fn_name)
    path_to_file = Path(inspect.getfile(fn))

    content = path_to_file.read_text()
    # make sure the file has the trailing newline
    assert content[-1] == '\n', 'expected a trailing newline character'

    if remove_trailing_newline:
        path_to_file.write_text(content[:-1])

    functions_reloaded = importlib.reload(functions)
    fn = getattr(functions_reloaded, fn_name)
    fn_source_original = inspect.getsource(fn)
    mod_source_original = path_to_file.read_text()

    with CallableInteractiveDeveloper(getattr(functions_reloaded, fn_name), {
            'upstream': None,
            'product': None
    }) as tmp_nb:
        pass

    functions_edited = importlib.reload(functions)
    fn_source_new = inspect.getsource(getattr(functions_edited, fn_name))
    mod_source_new = path_to_file.read_text()

    assert fn_source_original == fn_source_new
    assert mod_source_original == mod_source_new
    assert not Path(tmp_nb).exists()


def test_added_imports(backup_test_pkg):
    params = {'upstream': None, 'product': None}
    added_imports = 'import os\nimport sys\n'

    with CallableInteractiveDeveloper(functions.simple, params) as tmp_nb:
        nb = nbformat.read(tmp_nb, as_version=nbformat.NO_CONVERT)
        cell = find_cell_tagged(nb, 'imports-new')
        cell.source += f'\n{added_imports}'
        nbformat.write(nb, tmp_nb)

    content = Path(inspect.getfile(functions.simple)).read_text()
    assert added_imports in content


def test_error_if_source_is_modified_while_editing(backup_test_pkg):
    path_to_file = Path(inspect.getfile(functions.simple))

    with pytest.raises(ValueError) as excinfo:
        with CallableInteractiveDeveloper(functions.simple, {
                'upstream': None,
                'product': None
        }) as nb:
            path_to_file.write_text('')

    assert ('Changes from the notebook were not saved back to the module'
            in str(excinfo.value))
    assert Path(nb).exists()


def test_get_func_and_class_names():
    source = """
def x():\n    pass
\n
class A:\n    pass
"""

    assert set(interact.get_func_and_class_names(
        parso.parse(source))) == {'x', 'A'}


def test_make_import_from_definitions(monkeypatch):
    source = """
def x():\n    pass
\n
class A:\n    pass

def some_function():
    pass
"""
    mock_fn = Mock()
    mock_fn.__name__ = 'some_function'

    mock_mod = Mock()
    mock_mod.__name__ = 'some.module'

    monkeypatch.setattr(inspect, 'getmodule', lambda _: mock_mod)

    assert (interact.make_import_from_definitions(
        parso.parse(source), mock_fn) == 'from some.module import x, A')
