"""
Notes to developers

meta:
  Settings that cannot be clearly mapped to the OOP Python interface, we
  don't call it config because there is a DAGConfig object in the Python
  API and this might cause confusion


All other sections should represent valid DAG properties.
"""
import os
import sys
import yaml
import logging
from pathlib import Path
from collections.abc import MutableMapping

from ploomber import products
from ploomber import DAG, tasks
from ploomber.util.util import _load_factory, find_file_recursively
from ploomber.static_analysis import project
from ploomber.spec.TaskDict import TaskDict
from ploomber.spec import validate
from ploomber.dag.DAGConfiguration import DAGConfiguration
from ploomber.exceptions import DAGSpecInitializationError


logger = logging.getLogger(__name__)


class DAGSpec(MutableMapping):
    """
    A DAG spec is a dictionary with certain structure that can be converted
    to a DAG using DAGSpec.to_dag().

    There are two cases: the simplest one is just a dictionary with a
    "location" key with the factory to call, the other explicitely describes
    the DAG structure as a dictionary.
    """
    def __init__(self, data):
        # only set when initialized from DAGSpec.from_file
        self._parent_path = None

        if isinstance(data, list):
            data = {'tasks': data}

        load_from_factory = self._validate_top_level_keys(data)
        self.data = data

        if not load_from_factory:
            self.data['tasks'] = [normalize_task(task)
                                  for task in self.data['tasks']]
            self._validate_meta()

    def _validate_top_level_keys(self, spec):
        load_from_factory = False

        if 'location' in spec:
            if len(spec) > 1:
                raise KeyError('If specifying dag through a "location" key '
                               'it must be the unique key in the spec')
            else:
                load_from_factory = True
        else:
            valid = {'meta', 'config', 'clients', 'tasks'}
            validate.keys(valid, spec.keys(), name='dag spec')

        return load_from_factory

    def _validate_meta(self):
        if 'meta' not in self.data:
            self.data['meta'] = {}

        valid = {'extract_upstream', 'extract_product',
                 'product_default_class'}
        validate.keys(valid, self.data['meta'], name='dag spec')

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

    def to_dag(self):
        """Converts the DAG spec to a DAG object
        """
        cwd_old = os.getcwd()

        # when initializing DAGs from pipeline.yaml files, we have to ensure
        # that the parent folder is in sys.path for imports to work, this
        # happens most of the time but for some (unknown) reason, it doesn't
        # happen when initializing PloomberContentsManager.
        # pipeline.yaml paths are written relative to that file, for source
        # scripts to be located we temporarily change the current working
        # directory
        if self._parent_path is not None:
            sys.path.append(self._parent_path)
            os.chdir(self._parent_path)

        try:
            dag = self._to_dag()
        finally:
            if self._parent_path is not None:
                sys.path.remove(self._parent_path)
                os.chdir(cwd_old)
        return dag

    def _to_dag(self):
        # FIXME: validate that if there is location, there isn't anything else
        if 'location' in self:
            factory = _load_factory(self['location'])
            return factory()

        tasks = self.pop('tasks')

        dag = DAG()

        if 'config' in self:
            dag._params = DAGConfiguration.from_dict(self['config'])

        clients = self.get('clients')

        if clients:
            init_clients(dag, clients)

        process_tasks(dag, tasks, self)

        return dag

    @classmethod
    def auto_load(cls):
        """Looks for a pipeline.yaml, generates a DAGSpec and returns a DAG

        The pipeline.yaml parent folder is temporarily added to sys.path when
        calling DAGSpec.to_dag() to make sure imports work as expected
        """
        path = find_file_recursively('pipeline.yaml')

        if path is None:
            return None

        try:
            return cls.from_file(path).to_dag()
        except Exception as e:
            exc = DAGSpecInitializationError('Error initializing DAG from '
                                             'pipeline.yaml')
            raise exc from e

    @classmethod
    def from_file(cls, path):
        """
        Initialize dag spec with yaml file
        """
        with open(str(path)) as f:
            dag_dict = yaml.load(f, Loader=yaml.SafeLoader)

        spec = cls(dag_dict)
        spec._parent_path = str(Path(path).parent)
        return spec


def process_tasks(dag, tasks, dag_spec, root_path='.'):
    # determine if we need to run static analysis
    meta = dag_spec['meta']
    sources = [task_dict['source'] for task_dict in tasks]
    extracted = project.infer_from_path(root_path,
                                        templates=sources,
                                        upstream=meta['extract_upstream'],
                                        product=meta['extract_product'])

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


def normalize_task(task):
    if isinstance(task, str):
        return {'source': task}
    else:
        return task
