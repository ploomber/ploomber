import pytest
import pandas as pd

from ploomber import DAG, InMemoryDAG
from ploomber.tasks import PythonCallable, ShellScript
from ploomber.products import File
from ploomber.tasks import input_data_passer, in_memory_callable
from ploomber.exceptions import ValidationError


def _root(input_data):
    df = pd.DataFrame(input_data)
    return df


def _add_one(upstream):
    return upstream['root'] + 1


def _return_none(input_data):
    return None


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
                          params={'input_data': {
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

    out = dag_in_memory.build({'root': {'x': [1, 2, 3]}})

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

    PythonCallable(_return_none,
                   File('root.parquet'),
                   dag,
                   name='root',
                   params={'input_data': None},
                   serializer=serializer)

    dag_ = InMemoryDAG(dag)

    with pytest.raises(ValueError) as excinfo:
        dag_.build({'root': None})

    expected = ('All callables in a InMemoryDAG must return a value. '
                'Callable "_return_none", from task "root" returned None')
    assert str(excinfo.value) == expected


@pytest.mark.parametrize('copy', [True, False])
def test_copy(copy):
    def _assign_upstream(upstream):
        _assign_upstream.obj = upstream
        return 42

    dag_ = DAG()

    root = PythonCallable(_root,
                          File('root.parquet'),
                          dag_,
                          name='root',
                          serializer=serializer,
                          params={'input_data': {
                              'x': [0, 0, 0]
                          }})

    task = PythonCallable(_assign_upstream,
                          File('task.parquet'),
                          dag_,
                          name='task',
                          unserializer=unserializer,
                          serializer=serializer)

    root >> task

    dag = InMemoryDAG(dag_)

    out = dag.build({'root': {'x': [1]}}, copy=copy)

    # test that the function _assign_upstream received the same object
    # the task root returned in the upstream argument if copy is disabled.
    # if copying, then it should be a different object
    assert (_assign_upstream.obj['root'] is out['root']) is (not copy)


def test_input_data_passer():
    dag = DAG()

    root = input_data_passer(dag, name='root')
    task = PythonCallable(_add_one,
                          File('task.parquet'),
                          dag,
                          name='task',
                          unserializer=unserializer,
                          serializer=serializer)

    root >> task

    dag_ = InMemoryDAG(dag)

    assert dag_.build({'root': 1}) == {'root': 1, 'task': 2}


def test_in_memory_callable():
    dag = DAG()

    def add_some(upstream, to_add):
        return upstream['root'] + to_add

    root = input_data_passer(dag, name='root')
    task = in_memory_callable(add_some,
                              dag,
                              name='task',
                              params=dict(to_add=2))

    root >> task

    dag_ = InMemoryDAG(dag)

    assert dag_.build({'root': 1}) == {'root': 1, 'task': 3}


@pytest.mark.parametrize(
    'input_data',
    [
        {
            'extra': None
        },
        {
            # test case with missing params
        },
    ])
def test_error_input_data(input_data, dag):
    dag_ = InMemoryDAG(dag)

    with pytest.raises(ValidationError):
        dag_.build(input_data)
