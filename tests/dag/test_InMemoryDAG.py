import pandas as pd

from ploomber import DAG, InMemoryDAG
from ploomber.tasks import PythonCallable
from ploomber.products import File


# TODO: product should be optional when serializer is passed
def _root(product, input_):
    df = pd.DataFrame(input_)
    return df


def _task(upstream, product):
    df = upstream['root']
    return df + 1


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
                          params={'input_': {
                              'x': [1, 2, 3]
                          }})

    task = PythonCallable(_task,
                          File('task.parquet'),
                          dag,
                          name='task',
                          unserializer=unserializer,
                          serializer=serializer)

    root >> task

    dag_in_memory = InMemoryDAG(dag)

    dag_in_memory.build({'root': {'input_': {'x': [10, 20, 30]}}})
