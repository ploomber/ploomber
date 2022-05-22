from pathlib import Path

import yaml
import pytest
import jupytext
import nbformat
from jupyter_client.kernelspec import NoSuchKernel

from ploomber.spec import DAGSpec
from ploomber.tasks._params import Params
from ploomber.sources.notebooksource import (NotebookSource, is_python,
                                             inject_cell,
                                             determine_kernel_name,
                                             _cleanup_rendered_nb,
                                             add_parameters_cell, _to_nb_obj)
from ploomber.sources.nb_utils import find_cell_with_tag
from ploomber.products import File
from ploomber.exceptions import RenderError, SourceInitializationError


@pytest.fixture
def tmp_nbs_ipynb(tmp_nbs):
    """Modifies the nbs example to have one task with ipynb format
    """
    # modify the spec so it has one ipynb task
    with open('pipeline.yaml') as f:
        spec = yaml.safe_load(f)

    spec['tasks'][0]['source'] = 'load.ipynb'
    Path('pipeline.yaml').write_text(yaml.dump(spec))

    # generate notebook in ipynb format
    jupytext.write(jupytext.read('load.py'), 'load.ipynb')


# Functions for generating notebooks with different characteristics for testing


def new_nb(fmt=None,
           kname=None,
           klang=None,
           kdisplay=None,
           code='default',
           add_tag=True):

    if code == 'default':
        code = ['1', '2']

    nb = nbformat.v4.new_notebook()

    if kname or klang or kdisplay:
        nb.metadata = {'kernelspec': {}}

        if kname:
            nb.metadata.kernelspec['name'] = kname

        if klang:
            nb.metadata.kernelspec['language'] = klang

        if kdisplay:
            nb.metadata.kernelspec['display_name'] = kdisplay

    if isinstance(code, str):
        code = [code]

    if code:
        for i, code_ in enumerate(code):
            cell = nbformat.v4.new_code_cell(source=code_)

            if add_tag and i == 0:
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


def get_injected_cell(nb):
    injected = None

    for cell in nb['cells']:
        if 'injected-parameters' in cell['metadata'].get('tags', []):
            injected = cell

    return injected


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
        (new_nb(fmt='Rmd', klang='R', kname='ir', kdisplay='R'), 'Rmd', 'r'),
        # if there is nothing and it's an ipynb, assume python
        (new_nb(fmt='ipynb'), 'ipynb', 'python'),
    ])
def test_read_file(nb_str, ext, expected, tmp_directory):
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

    assert len(source._nb_obj_unrendered.cells) == 2

    assert (source._nb_obj_unrendered.metadata.kernelspec.language ==
            expected_lang)
    assert (
        source._nb_obj_unrendered.metadata.kernelspec.name == expected_kernel)


# trying out a few variants, looks like the official one is to have the comma
# and c('value')
# see: https://bookdown.org/yihui/rmarkdown-cookbook/dev-vector.html
@pytest.mark.parametrize('code', [
    "```{r tags=['parameters']}\n```\n```{r}\n```\n",
    "```{r, tags=['parameters']}\n```\n```{r}\n```\n",
    "```{r tags=c('parameters')}\n```\n```{r}\n```\n",
    "```{r, tags=c('parameters')}\n```\n```{r}\n```\n",
])
def test_rmd(code, tmp_directory):
    # check we can initialize from Rmd files with no kernelspec metadata,
    # we need this because the new_nb utility function always creates nbs
    # with metadata (because it uses jupytext.writes)
    path = Path('notebook.Rmd')
    path.write_text(code)
    source = NotebookSource(path)
    assert len(source._nb_obj_unrendered.cells) == 2


def test_kernelspec_overrides_nb_kernel_info():
    source = NotebookSource(new_nb(fmt='ipynb'),
                            ext_in='ipynb',
                            kernelspec_name='ir')
    assert source._nb_obj_unrendered.metadata.kernelspec.name == 'ir'
    assert source._nb_obj_unrendered.metadata.kernelspec.language == 'R'


def test_removes_papermill_metadata():
    source = NotebookSource(new_nb(fmt='ipynb'),
                            ext_in='ipynb',
                            kernelspec_name='python3')

    params = Params._from_dict({'product': File('file.ipynb')})
    source.render(params)
    nb = source.nb_obj_rendered

    assert all('papermill' not in cell['metadata'] for cell in nb.cells)


def test_error_if_kernelspec_name_is_invalid():
    with pytest.raises(NoSuchKernel):
        NotebookSource(new_nb(fmt='ipynb'),
                       ext_in='ipynb',
                       kernelspec_name='invalid_kernelspec')


def test_skip_kernelspec_install_check():
    NotebookSource(new_nb(fmt='ipynb'),
                   ext_in='ipynb',
                   kernelspec_name='unknown_kernelspec',
                   check_if_kernel_installed=False)


def test_error_if_missing_source_file():
    with pytest.raises(SourceInitializationError) as excinfo:
        NotebookSource(Path('some.py'))

    assert 'File does not exist' in str(excinfo.value)


def test_error_if_missing_source_file_suggest_scaffold(tmp_directory):
    Path('pipeline.yaml').touch()

    with pytest.raises(SourceInitializationError) as excinfo:
        NotebookSource(Path('some.py'))

    assert 'File does not exist' in str(excinfo.value)
    assert 'ploomber scaffold' in str(excinfo.value)


def test_error_if_source_is_dir(tmp_directory):
    Path('some.py').mkdir()

    with pytest.raises(SourceInitializationError) as excinfo:
        NotebookSource(Path('some.py'))

    assert 'Expected a file, got a directory' in str(excinfo.value)


def test_error_if_source_is_dir_suggest_scaffold(tmp_directory):
    Path('some.py').mkdir()
    Path('pipeline.yaml').touch()

    with pytest.raises(SourceInitializationError) as excinfo:
        NotebookSource(Path('some.py'))

    assert 'Expected a file, got a directory' in str(excinfo.value)
    assert 'ploomber scaffold' in str(excinfo.value)


def test_error_if_parameters_cell_doesnt_exist():
    with pytest.raises(SourceInitializationError) as excinfo:
        NotebookSource(new_nb(fmt='ipynb', add_tag=False), ext_in='ipynb')

    assert 'Notebook does not have a cell tagged "parameters"' in str(
        excinfo.value)


def test_error_missing_params_cell_shows_path_if_available(tmp_directory):
    path = Path('nb.ipynb')
    path.write_text(new_nb(fmt='ipynb', add_tag=False))

    with pytest.raises(SourceInitializationError) as excinfo:
        NotebookSource(path)

    assert ('Notebook "nb.ipynb" does not have a cell tagged "parameters"'
            in str(excinfo.value))


def test_nb_str_contains_kernel_info():
    source = NotebookSource(new_nb(fmt='ipynb'), ext_in='ipynb')
    nb = nbformat.reads(source._nb_str_unrendered,
                        as_version=nbformat.NO_CONVERT)
    assert (set(
        nb.metadata.kernelspec.keys()) == {'display_name', 'language', 'name'})


# Static analysis tests (Python only)


def test_ignores_static_analysis_if_non_python_file():
    source = NotebookSource(new_nb(fmt='r:light'), ext_in='R')
    params = Params._from_dict({'product': File('output.ipynb')})

    source.render(params)


def test_no_error_if_missing_product_or_upstream():
    code = """
# + tags=["parameters"]

# +
"""
    source = NotebookSource(code, ext_in='py', kernelspec_name='python3')

    params = Params._from_dict({'product': File('output.ipynb')})

    source.render(params)


@pytest.mark.parametrize('hot_reload', [True, False])
def test_static_analysis(hot_reload, tmp_directory):
    nb = jupytext.reads(notebook_ab, fmt='py:light')
    path = Path('nb.ipynb')
    path.write_text(jupytext.writes(nb, fmt='ipynb'))

    source = NotebookSource(path, hot_reload=hot_reload)

    params = Params._from_dict({
        'product': File('output.ipynb'),
        'a': 1,
        'b': 2
    })

    source.render(params)


def test_error_if_strict_and_missing_params():
    source = NotebookSource(notebook_ab,
                            ext_in='py',
                            kernelspec_name='python3',
                            static_analysis='strict')

    params = Params._from_dict({'product': File('output.ipynb'), 'a': 1})

    with pytest.raises(TypeError) as excinfo:
        source.render(params)

    assert "Missing params: 'b'" in str(excinfo.value)


def test_error_if_strict_and_passing_undeclared_parameter():
    source = NotebookSource(notebook_ab,
                            ext_in='py',
                            kernelspec_name='python3',
                            static_analysis='strict')

    params = Params._from_dict({
        'product': File('output.ipynb'),
        'a': 1,
        'b': 2,
        'c': 3
    })

    with pytest.raises(TypeError) as excinfo:
        source.render(params)

    assert "Unexpected params: 'c'" in str(excinfo.value)


def test_error_if_strict_and_using_undeclared_variable():
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
                            static_analysis='strict')

    params = Params._from_dict({
        'product': File('output.ipynb'),
        'a': 1,
        'b': 2
    })

    with pytest.raises(RenderError) as excinfo:
        source.render(params)

    assert "undefined name 'c'" in str(excinfo.value)


def test_error_if_strict_and_syntax_error():
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
                            static_analysis='strict')

    params = Params._from_dict({
        'product': File('output.ipynb'),
        'a': 1,
        'b': 2
    })

    with pytest.raises(SyntaxError) as excinfo:
        source.render(params)

    assert 'invalid syntax\n\nif\n\n  ^\n' in str(excinfo.value)


def test_error_if_strict_and_undefined_name():
    notebook_w_error = """
# + tags=['parameters']

# +
df.head()
"""
    source = NotebookSource(notebook_w_error,
                            ext_in='py',
                            kernelspec_name='python3',
                            static_analysis='strict')

    params = Params._from_dict({'product': File('output.ipynb')})

    with pytest.raises(RenderError) as excinfo:
        source.render(params)

    assert "undefined name 'df'" in str(excinfo.value)


# Parameter injection


@pytest.mark.parametrize('nb_str, ext', [
    (new_nb(fmt='py:light'), 'py'),
    (new_nb(fmt='py:percent'), 'py'),
    (new_nb(fmt='ipynb'), 'ipynb'),
    (new_nb(fmt='r:light'), 'R'),
    (new_nb(fmt='r:percent'), 'r'),
])
def test_injects_parameters_on_render(nb_str, ext):
    s = NotebookSource(nb_str, ext_in=ext)
    params = Params._from_dict({
        'some_param': 1,
        'product': File('output.ipynb')
    })
    s.render(params)

    nb = nbformat.reads(s.nb_str_rendered, as_version=nbformat.NO_CONVERT)

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


nb_case_1 = """# This is a markdown docstring

# + tags=["parameters"]
product = {'a': 'path/a.csv'}
upstream = {'some_key': 'some_value'}
""", 'py', ['some_key'], {
    'a': 'path/a.csv'
}

nb_case_2 = """'''This is a string docstring'''

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


def test_repr_from_str():
    source = NotebookSource(notebook_ab, ext_in='py')
    assert repr(source) == 'NotebookSource(loaded from string)'


def test_str():
    source = NotebookSource(notebook_ab, ext_in='py')
    source.render(Params._from_dict({'product':
                                     File('path/to/file/data.csv')}))
    assert str(source) == ('\na = 1\nb = 2\nproduct = None\na + b')


def test_str_ignores_injected_cell(tmp_directory):
    path = Path('nb.py')
    path.write_text(notebook_ab)
    source = NotebookSource(path)
    source.render(Params._from_dict(dict(a=42, product=File('file.txt'))))
    source.save_injected_cell()

    source = NotebookSource(path)

    # injected cell should not be considered part of the source code
    assert 'a = 42' not in str(source)


def test_repr_from_path(tmp_directory):
    path = Path(tmp_directory, 'nb.py')
    Path('nb.py').write_text(notebook_ab)
    source = NotebookSource(path)
    assert repr(source) == "NotebookSource('{}')".format(path)


@pytest.mark.parametrize('code, docstring', [
    [nb_case_1[0], 'This is a markdown docstring'],
    [nb_case_2[0], 'This is a string docstring'],
    [nb_case_3[0], ''],
])
def test_parse_docstring(code, docstring):
    source = NotebookSource(code, ext_in='py')
    assert source.doc == docstring


def test_inject_cell(tmp_directory):
    nb = jupytext.reads('', fmt=None)
    model = {'content': nb, 'name': 'script.py'}
    inject_cell(model,
                params=Params._from_dict({'product': File('output.ipynb')}))


def test_determine_kernel_name_from_ext(tmp_directory):
    nb = jupytext.reads('', fmt=None)
    assert determine_kernel_name(nb,
                                 kernelspec_name=None,
                                 ext='py',
                                 language=None) == 'python3'


def test_save_injected_cell(tmp_nbs):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    nb = jupytext.read('load.py')
    expected = '# + tags=["injected-parameters"]'

    assert expected not in Path('load.py').read_text()
    assert nb.metadata.get('ploomber') is None

    dag['load'].source.save_injected_cell()
    nb = jupytext.read('load.py')

    assert expected in Path('load.py').read_text()
    assert nb.metadata.ploomber.injected_manually


def test_save_injected_cell_ipynb(tmp_nbs_ipynb):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    nb = jupytext.read('load.py')
    expected = '"injected-parameters"'

    assert expected not in Path('load.ipynb').read_text()
    assert nb.metadata.get('ploomber') is None

    dag['load'].source.save_injected_cell()
    nb = jupytext.read('load.ipynb')

    assert expected in Path('load.ipynb').read_text()
    assert nb.metadata.ploomber.injected_manually


@pytest.mark.parametrize('prefix', [
    'nbs',
    'some/notebooks',
])
def test_save_injected_cell_in_paired_notebooks(tmp_nbs, prefix):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.pair(prefix)

    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.save_injected_cell()

    assert get_injected_cell(jupytext.read(Path(prefix, 'load.ipynb')))
    assert get_injected_cell(jupytext.read(Path('load.py')))


def test_remove_injected_cell(tmp_nbs):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.save_injected_cell()
    expected = '# + tags=["injected-parameters"]'

    assert expected in Path('load.py').read_text()

    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.remove_injected_cell()

    nb = jupytext.read('load.py')

    assert expected not in Path('load.py').read_text()
    assert nb.metadata.ploomber == {}


def test_remove_injected_cell_from_ipynb(tmp_nbs_ipynb):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.save_injected_cell()

    source = jupytext.read(dag['load'].source.loc)
    assert find_cell_with_tag(source, 'injected-parameters')

    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.remove_injected_cell()

    source = jupytext.read(dag['load'].source.loc)
    assert find_cell_with_tag(source, 'injected-parameters')


@pytest.mark.parametrize('prefix', [
    'nbs',
    'some/notebooks',
])
def test_remove_injected_cell_in_paired_notebooks(tmp_nbs, prefix):
    # pair notebooks
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.pair(prefix)

    # inject cell
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.save_injected_cell()

    # remove cell
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.remove_injected_cell()

    assert not get_injected_cell(jupytext.read(Path(prefix, 'load.ipynb')))
    assert not get_injected_cell(jupytext.read(Path('load.py')))


def test_format(tmp_nbs):
    pipeline = 'pipeline.yaml'
    dag = DAGSpec(pipeline).to_dag().render()

    assert '# + tags=["parameters"]' in Path('load.py').read_text()

    dag['load'].source.format(fmt='py:percent', entry_point=pipeline)

    assert '# %% tags=["parameters"]' in Path('load.py').read_text()


def test_format_with_extension_change(tmp_nbs):
    pipeline = 'pipeline.yaml'
    dag = DAGSpec(pipeline).to_dag().render()
    dag['load'].source.format(fmt='ipynb', entry_point=pipeline)

    assert not Path('load.py').exists()
    assert jupytext.read('load.ipynb')


def test_pair(tmp_nbs):
    dag = DAGSpec('pipeline.yaml').to_dag().render()

    dag['load'].source.pair(base_path='nbs')
    nb = jupytext.reads(Path('load.py').read_text(), fmt='py:light')

    assert Path('nbs', 'load.ipynb').is_file()
    assert nb.metadata.jupytext.formats == 'nbs//ipynb,py:light'


def test_sync(tmp_nbs):
    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.pair(base_path='nbs')

    nb = jupytext.reads(Path('load.py').read_text(), fmt='py:light')
    nb.cells.append(nbformat.v4.new_code_cell(source='x = 42'))
    jupytext.write(nb, 'load.py', fmt='py:light')

    dag = DAGSpec('pipeline.yaml').to_dag().render()
    dag['load'].source.sync()

    nb = jupytext.reads(Path('nbs', 'load.ipynb').read_text(), fmt='ipynb')

    assert nb.cells[-1]['source'] == 'x = 42'


@pytest.mark.parametrize('method, kwargs', [
    ['save_injected_cell', {}],
    ['remove_injected_cell', {}],
    ['format', {
        'fmt': 'py:light'
    }],
    ['pair', {
        'base_path': 'nbs'
    }],
    ['sync', {}],
])
def test_error_message_when_initialized_from_str(tmp_nbs, method, kwargs):
    source = NotebookSource("""
# + tags=["parameters"]
# -

# +
# -
""",
                            ext_in='py')

    source.render(Params._from_dict({'product': File('file.ipynb')}))

    with pytest.raises(ValueError) as excinfo:
        getattr(source, method)(**kwargs)

    expected = (f"Cannot use '{method}' if notebook was not "
                "initialized from a file")
    assert str(excinfo.value) == expected


remove_one = """# %% tags=["parameters"]
x = 1

# %% tags=["injected-parameters"]
x = 2
"""

remove_two = """# %% tags=["parameters"]
x = 100

# %% tags=["injected-parameters"]
x = 2

# %% tags=["debugging-settings"]
x = 3
"""

remove_all = """# %% tags=["injected-parameters"]
x = 2

# %% tags=["debugging-settings"]
x = 3
"""


@pytest.mark.parametrize('nb, expected_n, expected_source', [
    [remove_one, 1, ['x = 1']],
    [remove_two, 1, ['x = 100']],
    [remove_all, 0, []],
])
def test_cleanup_rendered_nb(nb, expected_n, expected_source):
    out = _cleanup_rendered_nb(jupytext.reads(nb))

    assert len(out['cells']) == expected_n
    assert [c['source'] for c in out['cells']] == expected_source


@pytest.mark.parametrize(
    'code, name, extract_product, extract_upstream, expected, expected_fmt', [
        ['', 'file.py', False, False, '', 'light'],
        ['', 'file.py', True, False, 'product = None', 'light'],
        ['', 'file.py', False, True, 'upstream = None', 'light'],
        [
            nbformat.writes(nbformat.v4.new_notebook()),
            'file.ipynb',
            False,
            False,
            '',
            'ipynb',
        ],
        [
            nbformat.writes(nbformat.v4.new_notebook()),
            'file.ipynb',
            False,
            True,
            'upstream = None',
            'ipynb',
        ],
        [
            '# %%\n1+1\n\n# %%\n2+2',
            'file.py',
            False,
            True,
            'upstream = None',
            'percent',
        ],
    ])
def test_add_parameters_cell(tmp_directory, code, name, extract_product,
                             extract_upstream, expected, expected_fmt):
    path = Path(name)
    path.write_text(code)
    nb_old = jupytext.read(path)

    add_parameters_cell(name,
                        extract_product=extract_product,
                        extract_upstream=extract_upstream)

    nb_new = jupytext.read(path)
    cell, idx = find_cell_with_tag(nb_new, 'parameters')

    # adds the cell at the top
    assert idx == 0

    # only adds one cell
    assert len(nb_old.cells) + 1 == len(nb_new.cells)

    # expected source content
    assert expected in cell['source']

    # keeps the same format
    fmt, _ = (('ipynb', None) if path.suffix == '.ipynb' else
              jupytext.guess_format(path.read_text(), ext=path.suffix))
    assert fmt == expected_fmt


@pytest.mark.parametrize('error, kwargs', [
    ['Failed to read notebook', dict()],
    ["Failed to read notebook from 'nb.ipynb'",
     dict(path='nb.ipynb')],
])
def test_to_nb_obj_error_if_corrupted_json(error, kwargs):

    with pytest.raises(SourceInitializationError) as excinfo:
        _to_nb_obj('', language='python', ext='ipynb', **kwargs)

    assert error in str(excinfo.value)
    repr_ = str(excinfo.getrepr())
    assert 'Notebook does not appear to be JSON' in repr_
    assert 'Expecting value: line 1 column 1 (char 0)' in repr_


@pytest.mark.parametrize('content, error', [
    [
        """
# + tags=["parameters"]
x = 1

y = 2
""",
        '# +',
    ],
    [
        """
# %% tags=["parameters"]
x = 1

y = 2
""",
        '# %%',
    ],
    [
        """
# %% tags=["parameters"]
x = 1

# %%

# %%
""",
        '# %%',
    ],
],
                         ids=[
                             'light',
                             'percent',
                             'empty-cells',
                         ])
def test_error_if_last_cell_in_script_is_the_parameters_cell(
        tmp_directory, content, error):
    Path('script.py').write_text(content)

    source = NotebookSource(Path('script.py'))

    with pytest.raises(SourceInitializationError) as excinfo:
        source.render(Params._from_dict({'product': File('stuff')}))

    assert 'script.py' in str(excinfo.value)
    assert error in str(excinfo.value)


def test_error_if_last_cell_in_nb_is_the_parameters_cell(tmp_directory):
    Path('nb.ipynb').write_text(new_nb(fmt='ipynb', code='# some code'))

    source = NotebookSource(Path('nb.ipynb'))

    with pytest.raises(SourceInitializationError) as excinfo:
        source.render(Params._from_dict({'product': File('stuff')}))

    assert 'nb.ipynb' in str(excinfo.value)
    assert 'Add a new cell with your code.' in str(excinfo.value)


def test_error_if_source_str_like_path(tmp_directory):
    Path('script.py').touch()

    with pytest.raises(ValueError) as excinfo:
        NotebookSource('script.py')

    assert 'Perhaps you meant passing a pathlib.Path object' in str(
        excinfo.value)
