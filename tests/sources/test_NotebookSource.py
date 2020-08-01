from pathlib import Path

import pytest
import jupytext
import nbformat
from jupyter_client.kernelspec import NoSuchKernel

from ploomber.tasks.Params import Params
from ploomber.sources.NotebookSource import NotebookSource, is_python
from ploomber.products import File
from ploomber.exceptions import RenderError, SourceInitializationError

# Functions for generating notebooks with different characteristics for testing


def new_nb(fmt=None, kname=None, klang=None, code='1+1', add_tag=True):
    nb = nbformat.v4.new_notebook()

    if kname or klang:
        nb.metadata = {'kernelspec': {}}

        if kname:
            nb.metadata.kernelspec['name'] = kname

        if klang:
            nb.metadata.kernelspec['language'] = klang

    if code:
        cell = nbformat.v4.new_code_cell(source=code)

        if add_tag:
            cell.metadata = {'tags': ['parameters']}

        nb.cells.append(cell)

    if fmt:
        nb = jupytext.writes(nb, fmt=fmt)

    return nb


def nb_python_meta(dumps=False):
    nb = nbformat.v4.new_notebook()
    nb.metadata = {'kernelspec': {'language': 'python'}}

    if dumps:
        nb = jupytext.writes(nb, fmt='py:light')

    return nb


def nb_python_inferred():
    nb = nbformat.v4.new_notebook()
    nb.metadata = {'kernelspec': {'language': 'python'}}
    code = nbformat.v4.new_code_cell(source='1+1')
    nb.cells.append(code)
    return nb


def nb_R_meta():
    nb = nbformat.v4.new_notebook()
    nb.metadata = {'kernelspec': {'language': 'R'}}
    return nb


notebook_ab = """
# + tags=['parameters']
a = 1
b = 2
product = None

# +
a + b
"""


@pytest.mark.parametrize(
    'nb_str, ext, expected',
    [
        (new_nb(fmt='py:light'), 'py', 'python'),
        (new_nb(fmt='r:light'), 'r', 'r'),
        (new_nb(fmt='r:light'), 'R', 'r'),
        (new_nb(fmt='ipynb', klang='python'), 'ipynb', 'python'),
        (new_nb(fmt='ipynb', klang='R', kname='ir'), 'ipynb', 'r'),
        # if there is nothing and it's an ipynb, assume python
        (new_nb(fmt='ipynb'), 'ipynb', 'python'),
    ])
def test_language_and_kernel(nb_str, ext, expected, tmp_directory):
    path = Path('nb.' + ext)
    path.write_text(nb_str)
    source = NotebookSource(path)

    assert source._ext_in == ext
    assert source.language == expected

    # jupyter sets as "R" (and not "r") as the language for R notebooks
    if expected == 'r':
        expected_lang = expected.upper()
    else:
        expected_lang = expected

    # most common kernels
    lang2kernel = {'python': 'python3', 'r': 'ir'}
    expected_kernel = lang2kernel[expected]

    assert source._nb_obj.metadata.kernelspec.language == expected_lang
    assert source._nb_obj.metadata.kernelspec.name == expected_kernel


def test_kernelspec_overrides_nb_kernel_info():
    source = NotebookSource(new_nb(fmt='ipynb'),
                            ext_in='ipynb',
                            kernelspec_name='ir')
    assert source._nb_obj.metadata.kernelspec.name == 'ir'
    assert source._nb_obj.metadata.kernelspec.language == 'R'


def test_error_if_kernelspec_name_is_invalid():
    with pytest.raises(NoSuchKernel):
        NotebookSource(new_nb(fmt='ipynb'),
                       ext_in='ipynb',
                       kernelspec_name='invalid_kernelspec')


def test_error_if_parameters_cell_doesnt_exist():
    with pytest.raises(SourceInitializationError) as excinfo:
        NotebookSource(new_nb(fmt='ipynb', add_tag=False), ext_in='ipynb')

    assert 'Notebook does not have a cell tagged "parameters"' in str(
        excinfo.value)


def test_nb_str_contains_kernel_info():
    source = NotebookSource(new_nb(fmt='ipynb'), ext_in='ipynb')
    nb = nbformat.reads(source._nb_repr, as_version=nbformat.NO_CONVERT)
    assert (set(
        nb.metadata.kernelspec.keys()) == {'display_name', 'language', 'name'})


# Static analysis tests (Pytnon only)


def test_error_if_static_analysis_on_a_non_python_nb():
    source = NotebookSource(new_nb(fmt='r:light'),
                            ext_in='R',
                            static_analysis=True)
    params = Params()
    params._dict = {'product': File('output.ipynb')}

    with pytest.raises(NotImplementedError):
        source.render(params)


def test_no_error_if_declared_and_passed_params_match():
    source = NotebookSource(notebook_ab,
                            ext_in='py',
                            kernelspec_name='python3',
                            static_analysis=True)

    params = Params()
    # notebook has params a and b, we are passing a and b as well
    params._dict = {'product': File('output.ipynb'), 'a': 1, 'b': 2}
    source.render(params)


def test_warn_if_using_default_value():
    source = NotebookSource(notebook_ab,
                            ext_in='py',
                            kernelspec_name='python3',
                            static_analysis=True)

    params = Params()
    params._dict = {'product': File('output.ipynb'), 'a': 1}

    with pytest.warns(UserWarning) as record:
        source.render(params)

    assert len(record) == 1
    assert (str(record[0].message) ==
            "Missing parameters: {'b'}, will use default value")


def test_error_if_passing_undeclared_parameter():
    source = NotebookSource(notebook_ab,
                            ext_in='py',
                            kernelspec_name='python3',
                            static_analysis=True)

    params = Params()
    params._dict = {'product': File('output.ipynb'), 'a': 1, 'b': 2, 'c': 3}

    with pytest.raises(RenderError) as excinfo:
        source.render(params)

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
    source = NotebookSource(notebook_w_warning,
                            ext_in='py',
                            kernelspec_name='python3',
                            static_analysis=True)

    params = Params()
    params._dict = {'product': File('output.ipynb'), 'a': 1, 'b': 2}

    with pytest.raises(RenderError) as excinfo:
        source.render(params)

    assert "undefined name 'c'" in str(excinfo.value)


def test_error_if_syntax_error():
    notebook_w_error = """
# + tags=['parameters']
a = 1
b = 2

# +
if
"""
    source = NotebookSource(notebook_w_error,
                            ext_in='py',
                            kernelspec_name='python3',
                            static_analysis=True)

    params = Params()
    params._dict = {'product': File('output.ipynb'), 'a': 1, 'b': 2}

    with pytest.raises(RenderError) as excinfo:
        source.render(params)

    assert 'invalid syntax' in str(excinfo.value)


# Parameter injection


@pytest.mark.parametrize('nb_str, ext', [
    (new_nb(fmt='py:light', code='# some code'), 'py'),
    (new_nb(fmt='py:percent', code='# some code'), 'py'),
    (new_nb(fmt='ipynb', code='# some code'), 'ipynb'),
    (new_nb(fmt='r:light', code='# some code'), 'R'),
    (new_nb(fmt='r:percent', code='# some code'), 'r'),
])
def test_injects_parameters_on_render(nb_str, ext):
    s = NotebookSource(nb_str, ext_in=ext)
    params = Params()
    params._dict = {'some_param': 1, 'product': File('output.ipynb')}
    s.render(params)

    nb = nbformat.reads(s.rendered_nb_str, as_version=nbformat.NO_CONVERT)

    # cell 0: parameters
    # cell 1: injected-parameters

    injected = nb.cells[1]
    tags = injected['metadata']['tags']

    assert len(tags) == 1
    assert tags[0] == 'injected-parameters'

    # py 3.5 does not gurantee order, so we check them separately
    assert 'some_param = 1' in injected['source']
    assert 'product = "output.ipynb"' in injected['source']


# extracting language from notebook objects


@pytest.mark.parametrize('nb, expected', [(nb_python_meta(), True),
                                          (nb_python_inferred(), True),
                                          (nb_R_meta(), False)])
def test_infer_language(nb, expected):
    assert is_python(nb) is expected


def nb_unknown(metadata):
    # second most popular case is R, but we don't have a way to know for sure
    nb = nbformat.v4.new_notebook()
    nb.metadata = metadata
    code = nbformat.v4.new_code_cell(source='a <- 1')
    nb.cells.append(code)
    return nb


@pytest.mark.parametrize('nb', [
    nb_unknown({}),
    nb_unknown({'kernelspec': {}}),
])
def test_error_with_unknown_language(nb):
    assert not is_python(nb)


nb_case_1 = """
# + tags=["parameters"]
product = {'a': 'path/a.csv'}
upstream = {'some_key': 'some_value'}
""", 'py', ['some_key'], {
    'a': 'path/a.csv'
}

nb_case_2 = """
# + tags=["parameters"]
upstream = {'a': None, 'b': None}
product = {'a': 'path/a.csv'}
""", 'py', ['a', 'b'], {
    'a': 'path/a.csv'
}

nb_case_3 = """
# + tags=["parameters"]
product = list(a="path/a.csv")
upstream <- list('a', 'b')
""", 'R', ['a', 'b'], {
    'a': 'path/a.csv'
}

nb_case_4 = """
# + tags=["parameters"]
upstream = list('x', 'y')
product  <-  list(a="path/a.csv")
""", 'r', ['x', 'y'], {
    'a': 'path/a.csv'
}


@pytest.mark.parametrize('code, ext, expected_up, expected_prod',
                         [nb_case_1, nb_case_2, nb_case_3, nb_case_4])
def test_extract_upstream_from_parameters(code, ext, expected_up,
                                          expected_prod):
    source = NotebookSource(code, ext_in=ext)
    upstream = source.extract_upstream()
    assert sorted(upstream) == sorted(expected_up)
    assert source.extract_product() == expected_prod
