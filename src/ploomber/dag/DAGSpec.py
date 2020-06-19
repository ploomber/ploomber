"""
Build DAGs from dictionaries

The Python API provides great flexibility to build DAGs but some users
not need all of this. This module implements functions to parse dictionaries
and instantiate DAGs, only simple use cases should be handled by this API,
otherwise the dictionary schema will be too complex, defeating the purpose.
"""
from pathlib import Path
from collections.abc import Mapping, Iterable

from ploomber import products
from ploomber import DAG, tasks
from ploomber.clients import SQLAlchemyClient
from ploomber.util.util import _load_factory


def has_value_at(d, dotted_path):
    current = d

    for key in dotted_path.split('.'):
        try:
            current = current[key]
        except KeyError:
            return False

    return True


def _make_iterable(o):
    if isinstance(o, Iterable) and not isinstance(o, str):
        return o
    elif o is None:
        return []
    else:
        return [o]


def _pop_upstream(task_dict):
    upstream = task_dict.pop('upstream', None)
    return _make_iterable(upstream)


def _pop_product(task_dict, dag_spec):
    product_raw = task_dict.pop('product')

    if has_value_at(dag_spec, 'meta.product_class'):
        CLASS = getattr(products, dag_spec['meta']['product_class'])
    else:
        CLASS = products.File

    if isinstance(product_raw, Mapping):
        return {key: CLASS(value) for key, value in product_raw.items()}
    else:
        return CLASS(product_raw)


def init_task(task_dict, dag, dag_spec):
    """Create a task from a dictionary
    """
    upstream = _pop_upstream(task_dict)
    class_raw = task_dict.pop('class')
    class_ = getattr(tasks, class_raw)

    product = _pop_product(task_dict, dag_spec)
    source_raw = task_dict.pop('source')
    name_raw = task_dict.pop('name', None)

    task = class_(source=Path(source_raw),
                  product=product,
                  name=name_raw or source_raw,
                  dag=dag,
                  **task_dict)

    return task, upstream


def init_dag(dag_spec):
    """Create a dag from a dictionary
    """
    if isinstance(dag_spec, Mapping):
        if 'location' in dag_spec:
            factory = _load_factory(dag_spec['location'])
            return factory()
        else:
            tasks = dag_spec.pop('tasks')

            dag = DAG()

            if has_value_at(dag_spec, 'config.clients'):
                init_clients(dag, dag_spec['config']['clients'])

            process_tasks(dag, tasks, dag_spec)

            return dag
    else:
        dag = DAG()
        process_tasks(dag, dag_spec, {})
        return dag


def process_tasks(dag, tasks, dag_spec):
    for task_dict in tasks:
        task, upstream = init_task(task_dict, dag, dag_spec)

        for task_up in upstream:
            task.set_upstream(dag[task_up])


def init_clients(dag, clients):
    for class_name, uri in clients.items():

        class_ = getattr(tasks, class_name, None)

        if not class_:
            class_ = getattr(products, class_name)

        dag.clients[class_] = SQLAlchemyClient(uri)
