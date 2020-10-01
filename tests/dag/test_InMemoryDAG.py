import pandas as pd

from ploomber import DAG, InMemoryDAG
from ploomber.tasks import PythonCallable
from ploomber.products import File


# TODO: product should be optional when serializer is passed
def _root(product, input_dict):
    df = pd.DataFrame(input_dict)
    return df


def _add_one(upstream, product):
    return upstream['root'] + 1


def unserializer(upstream_product):
    return pd.read_parquet(str(upstream_product))


def serializer(output, product):
    output.to_parquet(str(product))


def test_in_memory_dag():
    dag = DAG()

    root = PythonCallable(_root,
                          File('root.parquet'),
                          dag,
                          name='root',
                          serializer=serializer,
                          params={'input_dict': {
                              'x': [0, 0, 0]
                          }})

    task = PythonCallable(_add_one,
                          File('task.parquet'),
                          dag,
                          name='task',
                          unserializer=unserializer,
                          serializer=serializer)

    root >> task

    dag_in_memory = InMemoryDAG(dag)

    out = dag_in_memory.build({'root': {'input_dict': {'x': [1, 2, 3]}}})

    assert out['root']['x'].tolist() == [1, 2, 3]
    assert out['task']['x'].tolist() == [2, 3, 4]
