"""
Build DAGs from dictionaries
"""
import logging
import argparse
from pathlib import Path
from collections.abc import Mapping, Iterable

import yaml

from ploomber.products import File
from ploomber import DAG, tasks


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


def _pop_product(task_dict):
    product_raw = task_dict.pop('product')

    if isinstance(product_raw, Mapping):
        return {key: File(value) for key, value in product_raw.items()}
    elif isinstance(product_raw, str):
        return File(product_raw)
    else:
        return product_raw


def init_task(task_dict, dag):
    """Create a task from a dictionary
    """
    upstream = _pop_upstream(task_dict)
    class_raw = task_dict.pop('class')
    class_ = getattr(tasks, class_raw)

    product = _pop_product(task_dict)
    source_raw = task_dict.pop('source')
    name_raw = task_dict.pop('name', None)

    task = class_(source=Path(source_raw),
                  product=product,
                  name=name_raw or source_raw,
                  dag=dag,
                  **task_dict)

    return task, upstream


def init_dag(dag_dict):
    """Create a dag from a dictionary
    """
    dag = DAG()

    for task_dict in dag_dict:
        task, upstream = init_task(task_dict, dag)

        for task_up in upstream:
            task.set_upstream(dag[task_up])

    return dag


def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument('spec', help='Path to DAG spec')
    parser.add_argument('--log', help='Enables logging to stdout at the '
                        'specified level', default=None)
    parser.add_argument('--action', help='Action to perform, defaults to '
                        'status', default='status')
    args = parser.parse_args()

    if args.log is not None:
        logging.basicConfig(level=args.log)

    with open(args.spec) as f:
        dag_dict = yaml.load(f, Loader=yaml.SafeLoader)

    dag = init_dag(dag_dict)

    getattr(dag, args.action)()

    return dag


if __name__ == '__main__':
    dag = _main()
