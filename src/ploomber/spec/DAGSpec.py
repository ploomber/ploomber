"""
Build DAGs from dictionaries

The Python API provides great flexibility to build DAGs but some users
not need all of this. This module implements functions to parse dictionaries
and instantiate DAGs, only simple use cases should be handled by this API,
otherwise the dictionary schema will be too complex, defeating the purpose.

NOTE: CLI is implemented in the entry module
"""
import logging
from collections.abc import MutableMapping

from ploomber import products
from ploomber import DAG, tasks
from ploomber.util.util import _load_factory
from ploomber.static_analysis import project
from ploomber.spec.TaskDict import TaskDict

# TODO: make DAGSpec object which should validate schema and automatically
# fill with defaults all required but mussing sections, to avoid using
#  get_value_at

logger = logging.getLogger(__name__)


def normalize_task(task):
    if isinstance(task, str):
        return {'source': task}
    else:
        return task


class DAGSpec(MutableMapping):

    def __init__(self, data):
        if isinstance(data, list):
            data = {'tasks': data}

        data['tasks'] = [normalize_task(task) for task in data['tasks']]

        self.data = data
        self.validate_meta()

    def validate_meta(self):
        if 'meta' not in self.data:
            self.data['meta'] = {}

        if 'infer_upstream' not in self.data['meta']:
            self.data['meta']['infer_upstream'] = True

        if 'extract_product' not in self.data['meta']:
            self.data['meta']['extract_product'] = True

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        for key in self.data:
            yield key

    def __len__(self):
        return len(self.data)


def get_value_at(d, dotted_path):
    current = d

    for key in dotted_path.split('.'):
        try:
            current = current[key]
        except KeyError:
            return None

    return current

# TODO: make it a method in DAGSpec
def init_dag(dag_spec, root_path=None):
    """Create a dag from a spec
    """
    if 'location' in dag_spec:
        factory = _load_factory(dag_spec['location'])
        return factory()

    dag_spec = DAGSpec(dag_spec)

    tasks = dag_spec.pop('tasks')

    dag = DAG()

    config_clients = get_value_at(dag_spec, 'config.clients')

    if config_clients:
        init_clients(dag, config_clients)

    process_tasks(dag, tasks, dag_spec)

    return dag


def process_tasks(dag, tasks, dag_spec, root_path='.'):
    # determine if we need to run static analysis
    sources = [task_dict['source'] for task_dict in tasks]
    extracted = project.infer_from_path(root_path, templates=sources,
                                        upstream=dag_spec['meta']['infer_upstream'],
                                        product=dag_spec['meta']['extract_product'])

    upstream = {}

    for task_dict in tasks:
        source = task_dict['source']

        task_dict_obj = TaskDict(task_dict, dag_spec['meta'])

        if dag_spec['meta']['infer_upstream']:
            task_dict_obj['upstream'] = extracted['upstream'][source]

        if dag_spec['meta']['extract_product']:
            task_dict_obj['product'] = extracted['product'][source]

        task, up = task_dict_obj.init(dag)
        upstream[task] = up

    # once we added all tasks, set upstream dependencies
    for task in list(dag.values()):
        for task_dep in upstream[task]:
            task.set_upstream(dag[task_dep])


def init_clients(dag, clients):
    for class_name, dotted_path in clients.items():

        class_ = getattr(tasks, class_name, None)

        if not class_:
            class_ = getattr(products, class_name)

        dag.clients[class_] = _load_factory(dotted_path)()
