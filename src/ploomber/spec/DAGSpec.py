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
from ploomber.util.util import load_dotted_path, find_file_recursively
from ploomber.static_analysis import project
from ploomber.spec.TaskSpec import TaskSpec
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

            self.data['tasks'] = [TaskSpec(t, self.data['meta'])
                                  for t in self.data['tasks']]

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

        self.default_meta(self.data['meta'])

    @classmethod
    def default_meta(cls, meta=None):
        """Fill missing values in a meta dictionary
        """
        if meta is None:
            meta = {}

        valid = {'extract_upstream', 'extract_product',
                 'product_default_class', 'product_relative_to_source',
                 'jupyter_hot_reload'}
        validate.keys(valid, meta, name='dag spec')

        if 'extract_upstream' not in meta:
            meta['extract_upstream'] = True

        if 'extract_product' not in meta:
            meta['extract_product'] = False

        if 'product_relative_to_source' not in meta:
            meta['product_relative_to_source'] = False

        if 'jupyter_hot_reload' not in meta:
            meta['jupyter_hot_reload'] = False

        defaults = {'SQLDump': 'File', 'NotebookRunner': 'File',
                    'SQLScript': 'SQLRelation'}

        if 'product_default_class' not in meta:
            meta['product_default_class'] = defaults
        else:
            for class_, prod in defaults.items():
                if class_ not in meta['product_default_class']:
                    meta['product_default_class'][class_] = prod

        return meta

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
            factory = load_dotted_path(self['location'])
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
    def auto_load(cls, to_dag=True):
        """
        Looks for a pipeline.yaml, generates a DAGSpec and returns a DAG.
        Currently, this is only used by the PloomberContentsManager, this is
        not intended to be a public API since initializing specs from paths
        where we have to recursively look for a pipeline.yaml has some
        considerations regarding relative paths that make this confusing,
        inside the contents manager, all those things are all handled for that
        use case.

        The pipeline.yaml parent folder is temporarily added to sys.path when
        calling DAGSpec.to_dag() to make sure imports work as expected
        """
        path = find_file_recursively('pipeline.yaml')

        if path is None:
            if to_dag:
                return None, None, None
            else:
                return None, None

        try:
            spec = cls.from_file(path)

            if to_dag:
                return spec, spec.to_dag(), path
            else:
                return spec, path

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

        if dag_spec['meta']['extract_upstream']:
            task_dict['upstream'] = extracted['upstream'][source]

        if dag_spec['meta']['extract_product']:
            task_dict['product'] = extracted['product'][source]

        task, up = task_dict.to_task(dag)
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

        dag.clients[class_] = load_dotted_path(dotted_path)()


def normalize_task(task):
    if isinstance(task, str):
        return {'source': task}
    else:
        return task
