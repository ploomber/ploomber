from unittest.mock import Mock
from pathlib import Path

import pytest
import jupytext
import nbformat
import nbconvert

from ploomber import DAG, DAGConfigurator
from ploomber.tasks import NotebookRunner
from ploomber.products import File
from ploomber.exceptions import DAGBuildError
from ploomber.tasks import notebook


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


@pytest.mark.parametrize('output',
                         ['file.ipynb', 'file.pdf', 'file.html', 'file.md'])
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
1 + 1
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
# something

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
# some code

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
# some code

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


# TODO: we are not testing output, we have to make sure params are inserted
# correctly
@pytest.fixture
def tmp_dag(tmp_directory):
    dag = DAG()

    code = """
# + tags=["parameters"]
# some code

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
    def mock_jupyter_notebook(path, app, args):
        nb = jupytext.reads('2 + 2', fmt='py')
        nbformat.write(nb, path)

    monkeypatch.setattr(notebook, '_open_jupyter_notebook',
                        mock_jupyter_notebook)
    monkeypatch.setattr(notebook, '_save', lambda: True)

    tmp_dag['nb'].develop()
    path = str(tmp_dag['nb'].source.loc)

    assert Path(path).read_text().strip() == '2 + 2'


@pytest.mark.parametrize('app', ['notebook', 'lab'])
def test_develop_with_custom_args(app, tmp_dag, monkeypatch):
    mock = Mock()

    monkeypatch.setattr(notebook.subprocess, 'call', mock)
    monkeypatch.setattr(notebook, '_save', lambda: True)

    tmp_dag['nb'].develop(app=app, args='--port=8888; rm file')

    # make sure params are quoted to prevent code injection
    mock.assert_called_once_with([
        'jupyter', app, 'some_notebook-tmp.ipynb', '"--port=8888;"', '"rm"',
        '"file"'
    ])


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
# some code

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

    def mock_jupyter_notebook(path, app, args):
        nb = jupytext.reads('2 + 2', fmt='py')
        nbformat.write(nb, path)

    dag.render()

    monkeypatch.setattr(notebook, '_open_jupyter_notebook',
                        mock_jupyter_notebook)
    monkeypatch.setattr(notebook, '_save', lambda: True)

    t.develop()

    # source code must be updated
    assert str(t.source).strip() == '2 + 2'

    nb = nbformat.reads(t.source.rendered_nb_str,
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
                       product=File('out.html'),
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

    assert '2 + 2' in t.source.rendered_nb_str

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
