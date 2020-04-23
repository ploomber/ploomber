from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import NotebookRunner
from ploomber.tasks.notebook import _to_ipynb
from ploomber.products import File
from ploomber.exceptions import DAGBuildError


def test_warns_if_no_parameters_tagged_cell():
    source = """
1 + 1
    """

    with pytest.warns(UserWarning):
        _to_ipynb(source, '.py', 'python3')


def test_can_execute_from_ipynb(path_to_assets, tmp_directory):
    dag = DAG()

    NotebookRunner(path_to_assets / 'sample.ipynb',
                   product=File(Path(tmp_directory, 'out.ipynb')),
                   dag=dag,
                   name='nb')
    dag.build()


def test_can_execute_to_html(path_to_assets, tmp_directory):
    dag = DAG()

    NotebookRunner(path_to_assets / 'sample.ipynb',
                   product=File(Path(tmp_directory, 'out.html')),
                   dag=dag,
                   name='nb')
    dag.build()


def test_can_execute_from_py(path_to_assets, tmp_directory):
    dag = DAG()

    NotebookRunner(path_to_assets / 'sample.py',
                   product=File(Path(tmp_directory, 'out.ipynb')),
                   dag=dag,
                   kernelspec_name='python3',
                   name='nb')
    dag.build()


def test_can_execute_with_parameters(tmp_directory):
    dag = DAG()

    code = """
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

from pathlib import Path

Path(product['model']).touch()
    """

    product = {'nb': File(Path(tmp_directory, 'out.ipynb')),
               'model': File(Path(tmp_directory, 'model.pkl'))}

    NotebookRunner(code,
                   product=product,
                   dag=dag,
                   kernelspec_name='python3',
                   params={'var': 1},
                   ext_in='py',
                   nb_product_key='nb',
                   name='nb')
    dag.build()


def test_failing_notebook_saves_partial_result(tmp_directory):
    dag = DAG()

    code = """
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
