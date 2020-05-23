"""
Build DAGs from dictionaries
"""
from pathlib import Path
from ploomber.products import File
from ploomber import DAG, tasks


def init_task(task_dict, dag):
    """Create a task from a dictionary
    """
    upstream = task_dict.pop('upstream', [])
    class_raw = task_dict.pop('class')
    class_ = getattr(tasks, class_raw)

    product_raw = task_dict.pop('product')
    source_raw = task_dict.pop('source')
    name_raw = task_dict.pop('name')

    product = {key: File(value) for key, value in product_raw.items()}

    task = class_(source=Path(source_raw),
                  product=product,
                  name=name_raw,
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
