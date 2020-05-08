import pytest

from ploomber.sources.NotebookSource import check_notebook_source, _load_nb
from ploomber.sources.NotebookSource import NotebookSource
from ploomber.products import File


notebook_ab = """
# + tags=['parameters']
a = 1
b = 2

# +
a + b
"""


def test_error_if_parameters_cell_doesnt_exist():
    notebook_no_parameters_tag = """
    a + b
    """

    with pytest.raises(ValueError) as excinfo:
        check_notebook_source(notebook_no_parameters_tag, {'a': 1, 'b': 2})

    assert ('Notebook does not have a cell tagged "parameters"'
            == str(excinfo.value))


def test_returns_true_if_parameters_match():
    assert check_notebook_source(notebook_ab, {'a': 1, 'b': 2})


def test_warn_if_using_default_value():
    with pytest.warns(UserWarning) as record:
        check_notebook_source(notebook_ab, {'a': 1})

    assert len(record) == 1
    assert (str(record[0].message)
            == "Missing parameters: {'b'}, will use default value")


def test_error_if_passing_undeclared_parameter():
    with pytest.raises(ValueError) as excinfo:
        check_notebook_source(notebook_ab, {'a': 1, 'b': 2, 'c': 3})

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
    with pytest.raises(ValueError) as excinfo:
        check_notebook_source(notebook_w_warning, {'a': 1, 'b': 2})

    assert "undefined name 'c'" in str(excinfo.value)


def test_error_if_syntax_error():
    notebook_w_error = """
# + tags=['parameters']
a = 1
b = 2

# +
if
"""
    with pytest.raises(ValueError) as excinfo:
        check_notebook_source(notebook_w_error, {'a': 1, 'b': 2})

    assert 'invalid syntax' in str(excinfo.value)


def test_warns_if_no_parameters_tagged_cell():
    source = """
1 + 1
    """

    with pytest.warns(UserWarning):
        _load_nb(source, 'py', 'python3')


def test_tmp_file_is_deleted():
    pass


def test_parameters_are_added_on_render():
    pass


def test_render():
    s = NotebookSource("""
    x = 1
    """, ext_in='py', kernelspec_name='python3')
    from ploomber.tasks.Params import Params
    s.render(Params({'some_param': 1, 'product': File('output.ipynb')}))

    # s.path

    # s.path


# TODO: test different jupytext formats