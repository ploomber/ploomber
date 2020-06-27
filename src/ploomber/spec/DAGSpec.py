"""
Notes to developers

meta:
  Settings that cannot be clearly mapped to the OOP Python interface, we
  don't call it config because there is a DAGConfig object in the Python
  API and this might cause confusion


All other sections should represent valid DAG properties.
"""
import logging
from collections.abc import MutableMapping

from ploomber import products
from ploomber import DAG, tasks
from ploomber.util.util import _load_factory
from ploomber.static_analysis import project
from ploomber.spec.TaskDict import TaskDict
from ploomber.dag.DAGConfiguration import DAGConfiguration

# TODO: make DAGSpec object which should validate schema and automatically
# fill with defaults all required but missing sections

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

        if 'extract_upstream' not in self.data['meta']:
            self.data['meta']['extract_upstream'] = True

        if 'extract_product' not in self.data['meta']:
            self.data['meta']['extract_product'] = False

        defaults = {'SQLDump': 'File', 'NotebookRunner': 'File',
                    'SQLScript': 'SQLRelation'}

        if 'product_default_class' not in self.data['meta']:
            self.data['meta']['product_default_class'] = defaults
        else:
            for class_, prod in defaults.items():
                if class_ not in self.data['meta']['product_default_class']:
                    self.data['meta']['product_default_class'][class_] = prod

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

    if 'config' in dag_spec:
        dag._params = DAGConfiguration.from_dict(dag_spec['config'])

    clients = dag_spec.get('clients')

    if clients:
        init_clients(dag, clients)

    process_tasks(dag, tasks, dag_spec)

    return dag


def process_tasks(dag, tasks, dag_spec, root_path='.'):
    # determine if we need to run static analysis
    sources = [task_dict['source'] for task_dict in tasks]
    extracted = project.infer_from_path(root_path, templates=sources,
                                        upstream=dag_spec['meta']['extract_upstream'],
                                        product=dag_spec['meta']['extract_product'])

    upstream = {}

    for task_dict in tasks:
        source = task_dict['source']

        task_dict_obj = TaskDict(task_dict, dag_spec['meta'])

        if dag_spec['meta']['extract_upstream']:
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
