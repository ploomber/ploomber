import pytest
import pandas as pd

from ploomber import DAG, InMemoryDAG
from ploomber.tasks import PythonCallable, ShellScript
from ploomber.products import File


# TODO: product should be optional when serializer is passed
def _root(product, input_dict):
    df = pd.DataFrame(input_dict)
    return df


def _add_one(upstream, product):
    return upstream['root'] + 1


def _return_none(product):
    pass


def unserializer(upstream_product):
    return pd.read_parquet(str(upstream_product))


def serializer(output, product):
    output.to_parquet(str(product))


@pytest.fixture
def dag():
    dag_ = DAG()

    root = PythonCallable(_root,
                          File('root.parquet'),
                          dag_,
                          name='root',
                          serializer=serializer,
                          params={'input_dict': {
                              'x': [0, 0, 0]
                          }})

    task = PythonCallable(_add_one,
                          File('task.parquet'),
                          dag_,
                          name='task',
                          unserializer=unserializer,
                          serializer=serializer)

    root >> task

    return dag_


def test_in_memory_dag(dag):

    dag_in_memory = InMemoryDAG(dag)

    out = dag_in_memory.build({'root': {'input_dict': {'x': [1, 2, 3]}}})

    assert out['root']['x'].tolist() == [1, 2, 3]
    assert out['task']['x'].tolist() == [2, 3, 4]


def test_error_if_non_compatible_tasks():
    dag = DAG()
    ShellScript('touch {{product}}', File('file.txt'), dag, name='task')

    with pytest.raises(TypeError) as excinfo:
        InMemoryDAG(dag)

    expected = ('All tasks in the DAG must be PythonCallable, '
                'got unallowed types: ShellScript')
    assert str(excinfo.value) == expected


def test_error_if_a_task_returns_none():
    dag = DAG()

    PythonCallable(_return_none, File('root.parquet'), dag, name='root')

    dag_ = InMemoryDAG(dag)

    with pytest.raises(ValueError) as excinfo:
        dag_.build({'root': None})

    expected = ('All callables in a InMemoryDAG must return a value. '
                'Callable "_return_none", from task "root" returned None')
    assert str(excinfo.value) == expected


def test_not_copy():
    pass


def test_copy():
    pass


@pytest.mark.parametrize(
    'root_params',
    [
        {
            'extra': None
        },
        {
            # missing params
        },
    ])
def test_error_root_params(root_params, dag):
    dag_ = InMemoryDAG(dag)

    with pytest.raises(KeyError):
        dag_.build(root_params)
