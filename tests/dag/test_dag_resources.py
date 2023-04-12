"""
Test that DAGs with tasks where some of the params are resources
"""
from pathlib import Path

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.executors import Serial
from ploomber.constants import TaskStatus


def task_with_resource(product, resources_):
    Path(product).touch()


def test_outdated_if_resource_changes(tmp_directory):
    def make():
        dag = DAG(executor=Serial(build_in_subprocess=False))

        PythonCallable(
            task_with_resource,
            File("output"),
            dag,
            params=dict(resources_=dict(file="resource.txt")),
        )

        return dag

    Path("resource.txt").write_text("hello")

    # build for the first time
    dag = make()
    dag.build()

    # it should be up-to-date now
    dag_2 = make().render()
    assert dag_2["task_with_resource"].exec_status == TaskStatus.Skipped

    # should be outdated this time
    Path("resource.txt").write_text("bye")
    dag_3 = make()

    product = dag_3["task_with_resource"].product

    assert product._outdated_code_dependency()
    assert not product._outdated_data_dependencies()
