from pathlib import Path
import pytest

import jupytext
import nbformat

from ploomber.tasks.Params import Params
from ploomber.sources.NotebookSource import check_notebook, _get_parameters_cell
from ploomber.sources.NotebookSource import NotebookSource
from ploomber.products import File
from ploomber.exceptions import RenderError


notebook_ab = """
# + tags=['parameters']
a = 1
b = 2

# +
a + b
"""

notebook_ab = jupytext.reads(notebook_ab, fmt='py')


def test_error_if_parameters_cell_doesnt_exist():
    notebook_no_parameters_tag = """
    a + b
    """

    nb = jupytext.reads(notebook_no_parameters_tag, fmt='py')

    with pytest.raises(RenderError) as excinfo:
        check_notebook(nb, {'a': 1, 'b': 2})

    assert ('Notebook does not have a cell tagged "parameters"'
            == str(excinfo.value))


def test_returns_true_if_parameters_match():
    assert check_notebook(notebook_ab, {'a': 1, 'b': 2})


def test_warn_if_using_default_value():
    with pytest.warns(UserWarning) as record:
        check_notebook(notebook_ab, {'a': 1})

    assert len(record) == 1
    assert (str(record[0].message)
            == "Missing parameters: {'b'}, will use default value")


def test_error_if_passing_undeclared_parameter():
    with pytest.raises(RenderError) as excinfo:
        check_notebook(notebook_ab, {'a': 1, 'b': 2, 'c': 3})

    assert str(excinfo.value) == "\nPassed non-declared parameters: {'c'}"


def test_error_if_using_undeclared_variable():
    notebook_w_warning = """
# + tags=['parameters']
a = 1
b = 2

# +
# variable "c" is used but never declared!
a + b + c
"""

    notebook_w_warning = jupytext.reads(notebook_w_warning, fmt='py')

    with pytest.raises(RenderError) as excinfo:
        check_notebook(notebook_w_warning, {'a': 1, 'b': 2})

    assert "undefined name 'c'" in str(excinfo.value)


def test_error_if_syntax_error():
    notebook_w_error = """
# + tags=['parameters']
a = 1
b = 2

# +
if
"""

    notebook_w_error = jupytext.reads(notebook_w_error, fmt='py')

    with pytest.raises(RenderError) as excinfo:
        check_notebook(notebook_w_error, {'a': 1, 'b': 2})

    assert 'invalid syntax' in str(excinfo.value)


def test_error_if_no_parameters_cell():
    source = """
1 + 1
    """

    source = jupytext.reads(source, fmt='py')

    with pytest.raises(RenderError) as excinfo:
        _get_parameters_cell(source)

    assert 'Notebook does not have a cell tagged "parameters"' in str(excinfo.value)


def test_tmp_file_is_deleted():
    s = NotebookSource("""
# + tags=['parameters']
product = None
    """, ext_in='py', kernelspec_name='python3')

    s.render(Params({'product': File('output.ipynb')}))
    loc = s.loc_rendered

    assert Path(loc).exists()
    del s
    assert not Path(loc).exists()


def test_injects_parameters_on_render():
    s = NotebookSource("""
# + tags=['parameters']
product = None
some_param = 2
    """, ext_in='py', kernelspec_name='python3')
    s.render(Params({'some_param': 1, 'product': File('output.ipynb')}))

    format_ = nbformat.versions[nbformat.current_nbformat]
    nb = format_.reads(Path(s.loc_rendered).read_text())

    # cell 0: empty
    # cell 1: parameters
    # cell 2: injected-parameters

    injected = nb.cells[2]
    tags = injected['metadata']['tags']

    assert len(tags) == 1
    assert tags[0] == 'injected-parameters'

    expected = '# Parameters\nsome_param = 1\nproduct = "output.ipynb"\n'
    assert injected['source'] == expected


# def test_error_if_missing_parameters_cell():
#     s = NotebookSource("""
#     x = 1
#     """, ext_in='py', kernelspec_name='python3')
#     s.render(Params({'some_param': 1, 'product': File('output.ipynb')}))
