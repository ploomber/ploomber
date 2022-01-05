import sys
from unittest.mock import Mock
from pathlib import Path

import pytest
import jupytext
import nbformat
import nbconvert

from ploomber import DAG, DAGConfigurator
from ploomber.tasks import NotebookRunner
from ploomber.products import File
from ploomber.exceptions import DAGBuildError, DAGRenderError
from ploomber.tasks import notebook
from ploomber.executors import Serial


def fake_from_notebook_node(self, nb, resources):
    return bytes(42), None


def test_error_when_path_has_no_extension():
    with pytest.raises(ValueError) as excinfo:
        notebook.NotebookConverter('a')

    msg = 'Could not determine output format for product: "a"'
    assert msg in str(excinfo.value)


@pytest.mark.parametrize(
    'path, exporter',
    [('file.ipynb', None), ('file.pdf', nbconvert.exporters.pdf.PDFExporter),
     ('file.html', nbconvert.exporters.html.HTMLExporter),
     ('file.md', nbconvert.exporters.markdown.MarkdownExporter)])
def test_notebook_converter_get_exporter_from_path(path, exporter):
    converter = notebook.NotebookConverter(path)
    assert converter.exporter == exporter


@pytest.mark.parametrize(
    'exporter_name, exporter',
    [('ipynb', None), ('pdf', nbconvert.exporters.pdf.PDFExporter),
     ('html', nbconvert.exporters.html.HTMLExporter),
     ('md', nbconvert.exporters.markdown.MarkdownExporter),
     ('markdown', nbconvert.exporters.markdown.MarkdownExporter),
     ('slides', nbconvert.exporters.slides.SlidesExporter)])
def test_notebook_converter_get_exporter_from_name(exporter_name, exporter):
    converter = notebook.NotebookConverter('file.ext', exporter_name)
    assert converter.exporter == exporter


@pytest.mark.parametrize('output', [
    'file.ipynb', 'file.pdf',
    pytest.param(
        'file.html',
        marks=pytest.mark.xfail(
            sys.platform == 'win32',
            reason='nbconvert has a bug when exporting to HTML on windows')),
    'file.md'
])
def test_notebook_conversion(monkeypatch, output, tmp_directory):
    # we mock the method that does the conversion to avoid having to
    # intstall tex for testing
    monkeypatch.setattr(nbconvert.exporters.pdf.PDFExporter,
                        'from_notebook_node', fake_from_notebook_node)

    nb = nbformat.v4.new_notebook()
    cell = nbformat.v4.new_code_cell('1 + 1')
    nb.cells.append(cell)

    with open(output, 'w') as f:
        nbformat.write(nb, f)

    conv = notebook.NotebookConverter(output)
    conv.convert()


@pytest.mark.parametrize(
    'name, out_dir',
    [
        ['sample.py', '.'],
        ['sample.R', '.'],
        ['sample.ipynb', '.'],
        # check still works even if the folder does not exit yet
        ['sample.ipynb', 'missing_folder']
    ])
def test_execute_sample_nb(name, out_dir, tmp_sample_tasks):
    dag = DAG()

    NotebookRunner(Path(name),
                   product=File(Path(out_dir, name + '.out.ipynb')),
                   dag=dag)
    dag.build()


def _dag_simple(nb_params=True, params=None):
    path = Path('sample.py')

    if nb_params:
        path.write_text("""
# + tags=["parameters"]
a = None
b = 1
c = 'hello'
""")
    else:
        path.write_text("""
# + tags=["parameters"]
""")

    dag = DAG()
    NotebookRunner(path, product=File('out.ipynb'), dag=dag, params=params)
    return dag


def _dag_two_tasks(nb_params=True, params=None):
    root = Path('root.py')
    root.write_text("""
# + tags=["parameters"]
""")

    path = Path('sample.py')
    if nb_params:
        path.write_text("""
# + tags=["parameters"]
a = None
b = 1
c = 'hello'
""")
    else:
        path.write_text("""
# + tags=["parameters"]
""")

    dag = DAG()
    root = NotebookRunner(root, product=File('root.ipynb'), dag=dag)
    task = NotebookRunner(path,
                          product=File('out.ipynb'),
                          dag=dag,
                          params=params)
    root >> task
    return dag


def test_dag_r(tmp_directory):
    path = Path('sample.R')

    path.write_text("""
# + tags=["parameters"]
a <- NULL
b <- 1
c <- c(1, 2, 3)
""")

    dag = DAG()
    NotebookRunner(path, product=File('out.ipynb'), dag=dag, params=dict(z=1))

    # parameter extraction is not implemented but should not raise an error
    dag.render()


def test_render_error_on_syntax_error(tmp_directory):
    path = Path('sample.py')

    path.write_text("""
# + tags=["parameters"]
if
""")

    dag = DAG()
    NotebookRunner(path, product=File('out.ipynb'), dag=dag)

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    assert 'invalid syntax\n\nif\n\n  ^\n' in str(excinfo.value)


def test_render_error_on_undefined_name_error(tmp_directory):
    path = Path('sample.py')

    path.write_text("""
# + tags=["parameters"]

# +
df.head()
""")

    dag = DAG()
    NotebookRunner(path, product=File('out.ipynb'), dag=dag)

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    assert "undefined name 'df'" in str(excinfo.value)


def test_render_pass_on_missing_product_parameter(tmp_directory):
    path = Path('sample.py')

    path.write_text("""
# + tags=["parameters"]

# +
df = None
df.to_csv(product)
""")

    dag = DAG()
    NotebookRunner(path, product=File('out.ipynb'), dag=dag)

    # the render process injects the cell with the product variable so this
    # should not raise any errors, even if the raw source code does not contain
    # the product variable
    assert dag.render()


@pytest.mark.parametrize('code', [
    """
# + tags=["parameters"]


# +
import pandas as pd
df = pd.read_csv(upstream['root'])
""",
    """
# + tags=["parameters"]

# +
x

# +
import pandas as pd
df = pd.read_csv(upstream['root'])
""",
],
                         ids=['simple', 'multiple-undefined'])
def test_render_error_on_missing_upstream(tmp_directory, code):
    path = Path('sample.py')
    path.write_text(code)

    dag = DAG()
    NotebookRunner(path, product=File('out.ipynb'), dag=dag)

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    expected = ("undefined name 'upstream'. Did you forget"
                " to declare upstream dependencies?")
    assert expected in str(excinfo.value)


@pytest.mark.parametrize('factory', [_dag_simple, _dag_two_tasks])
def test_render_error_on_missing_params(tmp_directory, factory):
    dag = factory()

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    assert "Missing params: 'a', 'b', and 'c'" in str(excinfo.value)


@pytest.mark.parametrize('factory', [_dag_simple, _dag_two_tasks])
def test_render_error_on_unexpected_params(tmp_directory, factory):
    dag = factory(nb_params=False, params=dict(a=1, b=2, c=3))

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    assert "Unexpected params: 'a', 'b', and 'c'" in str(excinfo.value)


@pytest.mark.parametrize('factory', [_dag_simple, _dag_two_tasks])
def test_render_error_on_missing_and_unexpected_params(tmp_directory, factory):
    dag = factory(nb_params=True, params=dict(d=1, e=2, f=3))

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    assert "Unexpected params: 'd', 'e', and 'f'" in str(excinfo.value)
    assert "Missing params: 'a', 'b', and 'c'" in str(excinfo.value)


@pytest.mark.parametrize('code', [
    """
# + tags=["parameters"]
upstream = None
product = None
""", """
# + tags=["parameters"]
upstream = None
""", """
# + tags=["parameters"]
product = None
"""
])
def test_ignores_declared_product_and_upstream(tmp_directory, code):
    path = Path('sample.py')

    path.write_text(code)

    dag = DAG()
    NotebookRunner(path, product=File('out.ipynb'), dag=dag)
    dag.render()


@pytest.mark.xfail(
    sys.platform == 'win32',
    reason='nbconvert has a bug when exporting to HTML on windows')
def test_can_convert_to_html(tmp_sample_tasks):
    dag = DAG()

    NotebookRunner(Path('sample.ipynb'),
                   product=File(Path('out.html')),
                   dag=dag,
                   name='nb')
    dag.build()


def test_can_execute_with_parameters(tmp_directory):
    dag = DAG()

    code = """
# + tags=["parameters"]
var = None
    """

    NotebookRunner(code,
                   product=File(Path(tmp_directory, 'out.ipynb')),
                   dag=dag,
                   kernelspec_name='python3',
                   params={'var': 1},
                   ext_in='py',
                   name='nb')
    dag.build()


def test_can_execute_when_product_is_metaproduct(tmp_directory):
    dag = DAG()

    code = """
# + tags=["parameters"]
var = None

# +
from pathlib import Path

Path(product['model']).touch()
    """

    product = {
        'nb': File(Path(tmp_directory, 'out.ipynb')),
        'model': File(Path(tmp_directory, 'model.pkl'))
    }

    NotebookRunner(code,
                   product=product,
                   dag=dag,
                   kernelspec_name='python3',
                   params={'var': 1},
                   ext_in='py',
                   nb_product_key='nb',
                   name='nb')
    dag.build()


def test_raises_error_if_key_does_not_exist_in_metaproduct(tmp_directory):
    dag = DAG()

    product = {
        'some_notebook': File(Path(tmp_directory, 'out.ipynb')),
        'model': File(Path(tmp_directory, 'model.pkl'))
    }

    code = """
# + tags=["parameters"]
var = None

# +
    """

    with pytest.raises(KeyError) as excinfo:
        NotebookRunner(code,
                       product=product,
                       dag=dag,
                       kernelspec_name='python3',
                       params={'var': 1},
                       ext_in='py',
                       nb_product_key='nb',
                       name='nb')

    assert "Key 'nb' does not exist in product" in str(excinfo.value)


def test_failing_notebook_saves_partial_result(tmp_directory):
    dag = DAG()

    code = """
# + tags=["parameters"]
var = None

raise Exception('failing notebook')
    """

    # attempting to generate an HTML report
    NotebookRunner(code,
                   product=File('out.html'),
                   dag=dag,
                   kernelspec_name='python3',
                   params={'var': 1},
                   ext_in='py',
                   name='nb')

    # build breaks due to the exception
    with pytest.raises(DAGBuildError):
        dag.build()

    # but the file with ipynb extension exists to help debugging
    assert Path('out.ipynb').exists()


def test_error_if_wrong_exporter_name(tmp_sample_tasks):
    dag = DAG()

    with pytest.raises(ValueError) as excinfo:
        NotebookRunner(Path('sample.ipynb'),
                       product=File(Path('out.ipynb')),
                       dag=dag,
                       nbconvert_exporter_name='wrong_name')

    assert ('Could not find nbconvert exporter with name "wrong_name"'
            in str(excinfo.value))


def test_error_if_cant_find_exporter_name(tmp_sample_tasks):
    dag = DAG()

    with pytest.raises(ValueError) as excinfo:
        NotebookRunner(Path('sample.ipynb'),
                       product=File(Path('out.wrong_ext')),
                       dag=dag,
                       nbconvert_exporter_name=None)

    assert 'Could not find nbconvert exporter' in str(excinfo.value)


def test_skip_kernel_install_check(tmp_directory):
    dag = DAG()

    code = """
# + tags=["parameters"]
1 + 1
    """

    NotebookRunner(code,
                   product=File(Path(tmp_directory, 'out.ipynb')),
                   dag=dag,
                   kernelspec_name='unknown_kernel',
                   ext_in='py',
                   name='nb',
                   check_if_kernel_installed=False)
    dag.render()


def test_creates_parents(tmp_directory):
    dag = DAG()

    code = """
# + tags=["parameters"]
product = None

# +
from pathlib import Path
Path(product['file']).touch()
    """

    product = {
        'nb': File(Path(tmp_directory, 'another', 'nb', 'out.ipynb')),
        'file': File(Path(tmp_directory, 'another', 'data', 'file.txt')),
    }

    NotebookRunner(code, product=product, dag=dag, ext_in='py', name='nb')
    dag.build()


# TODO: we are not testing output, we have to make sure params are inserted
# correctly
@pytest.fixture
def tmp_dag(tmp_directory):
    dag = DAG()

    code = """
# + tags=["parameters"]
var = None

# +
1 + 1
    """
    p = Path('some_notebook.py')

    p.write_text(code)

    NotebookRunner(p,
                   product=File(Path(tmp_directory, 'out.ipynb')),
                   dag=dag,
                   kernelspec_name='python3',
                   params={'var': 1},
                   name='nb')

    dag.render()

    return dag


def test_develop_saves_changes(tmp_dag, monkeypatch):
    def mock_jupyter_notebook(args, check):
        nb = jupytext.reads('2 + 2', fmt='py')
        # args: "jupyter" {app} {path} {other args, ...}
        nbformat.write(nb, args[2])

    monkeypatch.setattr(notebook.subprocess, 'run', mock_jupyter_notebook)
    monkeypatch.setattr(notebook, '_save', lambda: True)

    tmp_dag['nb'].develop()
    path = str(tmp_dag['nb'].source.loc)

    assert Path(path).read_text().strip() == '2 + 2'


@pytest.mark.parametrize('app', ['notebook', 'lab'])
def test_develop_with_custom_args(app, tmp_dag, monkeypatch):
    mock = Mock()

    monkeypatch.setattr(notebook.subprocess, 'run', mock)
    monkeypatch.setattr(notebook, '_save', lambda: True)

    tmp_dag['nb'].develop(app=app, args='--port=8888 --no-browser')

    # make sure params are quoted to prevent code injection
    mock.assert_called_once_with([
        'jupyter', app, 'some_notebook-tmp.ipynb', '--port=8888',
        '--no-browser'
    ],
                                 check=True)


def test_develop_unknown_app(tmp_dag):
    with pytest.raises(ValueError) as excinfo:
        tmp_dag['nb'].develop(app='unknown')

    assert '"app" must be one of' in str(excinfo.value)


def test_develop_workflow_with_hot_reload(tmp_directory, monkeypatch):
    cfg = DAGConfigurator()
    cfg.params.hot_reload = True
    dag = cfg.create()

    code = """
# + tags=["parameters"]
var = None

# +
1 + 1
    """
    p = Path('some_notebook.py')

    p.write_text(code)

    t = NotebookRunner(p,
                       product=File(Path(tmp_directory, 'out.ipynb')),
                       dag=dag,
                       kernelspec_name='python3',
                       params={'var': 1},
                       name='nb')

    def mock_jupyter_notebook(args, check):
        nb = jupytext.reads("""
# + tags=["parameters"]
var = None

# +
2 + 2
""",
                            fmt='py')
        # args: "jupyter" {app} {path} {others, ...}
        nbformat.write(nb, args[2])

    dag.render()

    monkeypatch.setattr(notebook.subprocess, 'run', mock_jupyter_notebook)
    monkeypatch.setattr(notebook, '_save', lambda: True)

    t.develop()

    # source code must be updated
    assert '2 + 2' in str(t.source).strip()

    nb = nbformat.reads(t.source.nb_str_rendered,
                        as_version=nbformat.NO_CONVERT)
    source = jupytext.writes(nb, fmt='py')

    assert '2 + 2' in source


def test_develop_error_if_r_notebook(tmp_sample_tasks):
    dag = DAG()

    t = NotebookRunner(Path('sample.R'), product=File('out.ipynb'), dag=dag)

    dag.render()

    with pytest.raises(NotImplementedError):
        t.develop()

    with pytest.raises(NotImplementedError):
        t.debug()


# TODO: make a more general text and parametrize by all task types
# but we also have to test it at the source level
# also test at the DAG level, we have to make sure the property that
# code differ uses (raw code) it also hot_loaded
def test_hot_reload(tmp_directory):
    cfg = DAGConfigurator()
    cfg.params.hot_reload = True

    dag = cfg.create()

    path = Path('nb.py')
    path.write_text("""
# + tags=["parameters"]
# some code

# +
1 + 1
    """)

    t = NotebookRunner(path,
                       product=File('out.ipynb'),
                       dag=dag,
                       kernelspec_name='python3')

    t.render()

    path.write_text("""
# + tags=["parameters"]
# some code

# +
2 + 2
    """)

    t.render()

    assert '2 + 2' in str(t.source)
    assert t.product._outdated_code_dependency()
    assert not t.product._outdated_data_dependencies()

    assert '2 + 2' in t.source.nb_str_rendered

    report = dag.build()

    assert report['Ran?'] == [True]

    # TODO: check task is not marked as outdated


@pytest.mark.parametrize('kind, to_patch', [
    ['ipdb', 'IPython.terminal.debugger.Pdb.run'],
    ['pdb', 'pdb.run'],
    ['pm', None],
])
def test_debug(monkeypatch, kind, to_patch, tmp_dag):
    if to_patch:
        mock = Mock()
        monkeypatch.setattr(to_patch, mock)

    tmp_dag['nb'].debug(kind=kind)

    if to_patch:
        mock.assert_called_once()


@pytest.mark.xfail(sys.platform == "win32",
                   reason="Two warnings are displayed on windows")
def test_warns_if_export_args_but_ipynb_output(tmp_sample_tasks):
    dag = DAG(executor=Serial(build_in_subprocess=False))

    NotebookRunner(Path('sample.ipynb'),
                   File('out.ipynb'),
                   dag,
                   nbconvert_export_kwargs=dict(exclude_input=True))

    with pytest.warns(UserWarning) as records:
        dag.build()

    # NOTE: not sure why sometimes two records are displayed, maybe another
    # library is throwing the warning
    assert any(
        "Output 'out.ipynb' is a notebook file" in record.message.args[0]
        for record in records)


def test_change_static_analysis(tmp_sample_tasks):
    dag = DAG(executor=Serial(build_in_subprocess=False))

    # static_analysis is True by default, this should fail
    t = NotebookRunner(Path('sample.ipynb'),
                       File('out.ipynb'),
                       dag,
                       params=dict(a=1, b=2))

    # disable it
    t.static_analysis = False

    # this should work
    dag.render()
