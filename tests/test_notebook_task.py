from pathlib import Path

import pytest

from ploomber import DAG, DAGConfigurator
from ploomber.tasks import NotebookRunner
from ploomber.products import File
from ploomber.exceptions import DAGBuildError


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


def test_raises_error_if_key_does_not_exist_in_metaproduct(tmp_directory):
    dag = DAG()

    product = {'some_notebook': File(Path(tmp_directory, 'out.ipynb')),
               'model': File(Path(tmp_directory, 'model.pkl'))}

    with pytest.raises(KeyError) as excinfo:
        NotebookRunner('',
                       product=product,
                       dag=dag,
                       kernelspec_name='python3',
                       params={'var': 1},
                       ext_in='py',
                       nb_product_key='nb',
                       name='nb')

    assert 'Key "nb" does not exist in product' in str(excinfo.value)


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


# TODO: we are not testing output, we have to make sure params are inserted
# correctly

# def test_develop(tmp_directory):
#     dag = DAG()

#     code = """
#     1 + 1
#     """
#     p = Path('some_notebook.py')

#     p.write_text(code)

#     t = NotebookRunner(p,
#                        product=File(Path(tmp_directory, 'out.ipynb')),
#                        dag=dag,
#                        kernelspec_name='python3',
#                        params={'var': 1},
#                        name='nb')
#     t.develop()


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
1 + 1
    """)

    t = NotebookRunner(path,
                       product=File('out.html'),
                       dag=dag,
                       kernelspec_name='python3')

    t.render()

    path.write_text("""
2 + 2
    """)

    t.render()

    assert '2 + 2' in str(t.source)
    assert t.product._outdated_code_dependency()
    assert not t.product._outdated_data_dependencies()

    assert '2 + 2' in Path(t.source.loc_rendered).read_text()

    report = dag.build()

    assert report['Ran?'] == [True]

    # TODO: check task is not marked as outdated
