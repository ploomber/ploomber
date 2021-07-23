"""
Test that DAGs with tasks where some of the params are resources
"""
from pathlib import Path

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.executors import Serial


def task_with_resource(product, resource__file):
    Path(product).touch()


def test_outdated_if_resource_changes(tmp_directory):
    def make():
        dag = DAG(executor=Serial(build_in_subprocess=False))

        PythonCallable(task_with_resource,
                       File('output'),
                       dag,
                       params=dict(resource__file='resource.txt'))

        return dag

    Path('resource.txt').write_text('hello')

    dag = make()
    dag.build()

    Path('resource.txt').write_text('bye')

    dag_ = make()

    product = dag_['task_with_resource'].product

    assert product._outdated_code_dependency()
    assert not product._outdated_data_dependencies()
