"""
Build DAGs from dictionaries

The Python API provides great flexibility to build DAGs but some users
not need all of this. This module implements functions to parse dictionaries
and instantiate DAGs, only simple use cases should be handled by this API,
otherwise the dictionary schema will be too complex, defeating the purpose.
"""
from pathlib import Path
from collections.abc import MutableMapping, Mapping, Iterable

from ploomber import products
from ploomber import DAG, tasks
from ploomber.clients import SQLAlchemyClient
from ploomber.util.util import _load_factory
from ploomber.static_analysis import project

# TODO: make DAGSpec object which should validate schema and automatically
# fill with defaults all required but mussing sections, to avoid using
#  get_value_at


class DAGSpec(MutableMapping):

    def __init__(self, data):
        self.data = data
        self.validate_meta()

    def validate_meta(self):
        if 'meta' not in self.data:
            self.data['meta'] = {}

        if 'infer_upstream' not in self.data['meta']:
            self.data['meta']['infer_upstream'] = True

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

    product_class = get_value_at(dag_spec, 'meta.product_class')

    if product_class:
        CLASS = getattr(products, product_class)
    else:
        CLASS = products.File

    if isinstance(product_raw, Mapping):
        return {key: CLASS(value) for key, value in product_raw.items()}
    else:
        return CLASS(product_raw)


suffix2class = {
    '.py': tasks.NotebookRunner,
    '.sql': tasks.SQLScript,
    '.sh': tasks.ShellScript
}


def get_task_class(task_dict):
    """
    Pops 'class' key if it exists

    Task class is determined by the 'class' key, if missing. Defaults
    are used by inspecting the 'source' key: NoteboonRunner (.py),
    SQLScript (.sql) and BashScript (.sh).
    """
    class_name = task_dict.pop('class', None)

    if class_name:
        class_ = getattr(tasks, class_name)
    else:
        suffix = Path(task_dict['source']).suffix

        if suffix2class.get(suffix):
            class_ = suffix2class[suffix]
        else:
            raise KeyError('No default available for task with source: '
                           '"{}". Default class is only available for '
                           'files with extensions {}, otherwise you should '
                           'set an explicit class key'
                           .format(task_dict['source'], set(suffix2class)))

    return class_


def init_task(task_dict, dag, dag_spec):
    """Create a task from a dictionary

    """
    upstream = _pop_upstream(task_dict)
    class_ = get_task_class(task_dict)

    product = _pop_product(task_dict, dag_spec)
    source_raw = task_dict.pop('source')
    name_raw = task_dict.pop('name', None)

    task = class_(source=Path(source_raw),
                  product=product,
                  name=name_raw or source_raw,
                  dag=dag,
                  **task_dict)

    return task, upstream


def init_dag(dag_spec, root_path=None):
    """Create a dag from a dictionary
    """
    if isinstance(dag_spec, Mapping):
        if 'location' in dag_spec:
            factory = _load_factory(dag_spec['location'])
            return factory()
        else:
            # validate and initialize defaults
            dag_spec = DAGSpec(dag_spec)

            tasks = dag_spec.pop('tasks')

            dag = DAG()

            config_clients = get_value_at(dag_spec, 'config.clients')

            if config_clients:
                init_clients(dag, config_clients)

            process_tasks(dag, tasks, dag_spec)

            return dag
    else:
        dag = DAG()
        process_tasks(dag, dag_spec, {}, root_path)
        return dag


def process_tasks(dag, tasks, dag_spec, root_path='.'):
    for task_dict in tasks:
        task, upstream = init_task(task_dict, dag, dag_spec)

        infer_upstream = get_value_at(dag_spec, 'meta.infer_upstream')
        # TODO: validate. if infer_upstream, tasks should not have upstream
        # key

        if not infer_upstream:
            for task_up in upstream:
                task.set_upstream(dag[task_up])

    if infer_upstream:
        # FIXME: make the path relative root_path for this to work in all
        # cases
        # get all arguments used to initialize tasks (template names)
        templates = [str(task.source.loc) for task in dag.values()]
        dependencies = project.infer_depencies_from_path(root_path,
                                                         templates=templates)
        for name, upstream in dependencies.items():
            for task_up in upstream:
                dag[name].set_upstream(dag[task_up])


def init_clients(dag, clients):
    for class_name, uri in clients.items():

        class_ = getattr(tasks, class_name, None)

        if not class_:
            class_ = getattr(products, class_name)

        dag.clients[class_] = SQLAlchemyClient(uri)
