from pathlib import Path

from ploomber.dag import DAG
from ploomber.tasks import NotebookRunner
from ploomber.products import File


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

    code = """product = None

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
