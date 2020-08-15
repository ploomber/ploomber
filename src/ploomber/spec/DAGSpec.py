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
from glob import iglob
from itertools import chain
import pprint

from ploomber import products
from ploomber import DAG, tasks, SourceLoader
from ploomber.util.util import (load_dotted_path, find_file_recursively,
                                call_with_dictionary)
from ploomber.spec.TaskSpec import TaskSpec, suffix2taskclass
from ploomber.spec import validate
from ploomber.dag.DAGConfiguration import DAGConfiguration
from ploomber.exceptions import DAGSpecInitializationError
from ploomber.env.expand import expand_raw_dictionary
from ploomber.tasks import NotebookRunner

logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)


class DAGSpec(MutableMapping):
    """
    A DAG spec is a dictionary with certain structure that can be converted
    to a DAG using DAGSpec.to_dag().

    There are two cases: the simplest one is just a dictionary with a
    "location" key with the factory to call, the other explicitly describes
    the DAG structure as a dictionary.

    Notes
    -----
    Relative paths are heavily used for pointing to source files and products,
    before the introduction of the Spec API, this was unambiguous: all paths
    were relative to the current working directory, you only had to make sure
    to start the session at the right place. Initially, the Spec API didn't
    change a thing since the assumption was that one would convert a spec to
    a DAG in a folder containing a pipeline.yaml file and paths in such file
    would be relative to the file itself, no ambiguity. However, when we
    introduced the Jupyter custom contents manager things changed because the
    DAG can be initialized from directories where there isn't a pipeline.yaml
    but we have to recursively look for one. Furthermore, if extract_upstream
    or extract_product are True and the source files are not in the same folder
    as pipeline.yaml an ambiguity arises: are paths inside a source file
    relative to the source file or pipeline.yaml? To make this consistent,
    all relative paths (whether in a pipeline.yaml or in a source file) are
    relative to the pipeline.yaml folder but this can be changed with the
    product_relative_to_source option. Furthermore, when usign the custom
    contents manager, we convert relative paths to absolute in special cases
    to make sure they work. This is why we need to keep the pipeline.yaml
    location when initializing a DAG from a spec.
    """
    def __init__(self, data, env=None):
        # only set when initialized from DAGSpec.from_file
        self._parent_path = None

        if isinstance(data, list):
            data = {'tasks': data}

        logger.debug('DAGSpec enviroment:\n%s', pp.pformat(env))

        # expand if there's an env.yaml file
        if env is not None:
            data = expand_raw_dictionary(data, env)

        logger.debug('Expanded DAGSpec:\n%s', pp.pformat(data))

        load_from_factory = self._validate_top_level_keys(data)
        self.data = data

        if not load_from_factory:
            self.data['tasks'] = [
                normalize_task(task) for task in self.data['tasks']
            ]
            self._validate_meta()

            self.data['tasks'] = [
                TaskSpec(t, self.data['meta']) for t in self.data['tasks']
            ]

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

        self.data['meta'] = Meta.default_meta(self.data['meta'])

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
            sys.path.insert(0, os.path.abspath(self._parent_path))
            os.chdir(self._parent_path)

        try:
            dag = self._to_dag()
        finally:
            if self._parent_path is not None:
                sys.path.remove(os.path.abspath(self._parent_path))
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

        process_tasks(dag, tasks, self, root_path=self._parent_path)

        return dag

    @classmethod
    def auto_load(cls, to_dag=True, starting_dir=None):
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
        path = find_file_recursively('pipeline.yaml',
                                     starting_dir=starting_dir or os.getcwd())

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
    def from_directory(cls, path_to_dir):
        """Construct a DAGSpec from a directory

        Look for scripts (``.py``, ``.R`` or ``.ipynb``) in the directory and
        interpret them as task sources, file names are assigned as task names
        (without extension). The spec is generated with the default values in
        the "meta" section.
        """
        extensions = [
            name for name, class_ in suffix2taskclass.items()
            if class_ is NotebookRunner
        ]

        files = chain.from_iterable(iglob('*' + ext) for ext in extensions)

        tasks = [{
            'source': file_,
            'name': str(Path(file_).with_suffix(''))
        } for file_ in files]

        return cls({'tasks': tasks})

    @classmethod
    def from_file(cls, path, env=None):
        """
        Initialize dag spec with yaml file
        """
        with open(str(path)) as f:
            dag_dict = yaml.load(f, Loader=yaml.SafeLoader)

        spec = cls(dag_dict, env=env)
        spec._parent_path = str(Path(path).parent)
        return spec


class Meta:
    """Schema for meta section in pipeline.yaml
    """
    @classmethod
    def default_meta(cls, meta=None):
        """Fill missing values in a meta dictionary
        """
        if meta is None:
            meta = {}

        valid = {
            'extract_upstream', 'extract_product', 'product_default_class',
            'product_relative_to_source', 'jupyter_hot_reload', 'source_loader'
        }
        validate.keys(valid, meta, name='dag spec')

        if 'extract_upstream' not in meta:
            meta['extract_upstream'] = True

        if 'extract_product' not in meta:
            meta['extract_product'] = True

        if 'product_relative_to_source' not in meta:
            meta['product_relative_to_source'] = False

        if 'jupyter_hot_reload' not in meta:
            meta['jupyter_hot_reload'] = False

        if 'source_loader' not in meta:
            meta['source_loader'] = None
        else:
            meta['source_loader'] = SourceLoader(**meta['source_loader'])

        defaults = {
            'SQLDump': 'File',
            'NotebookRunner': 'File',
            'SQLScript': 'SQLRelation'
        }

        if 'product_default_class' not in meta:
            meta['product_default_class'] = defaults
        else:
            for class_, prod in defaults.items():
                if class_ not in meta['product_default_class']:
                    meta['product_default_class'][class_] = prod

        return meta


def process_tasks(dag, tasks, dag_spec, root_path=None):
    """
    Initialize Task objects from TaskSpec, extract product and dependencies
    if needed and set the dag dependencies structure
    """
    root_path = root_path or '.'

    upstream = {}
    source_obj = {}
    extract_up = dag_spec['meta']['extract_upstream']
    extract_prod = dag_spec['meta']['extract_product']

    # first pass: init tasks and them to dag
    for task_dict in tasks:
        fn = task_dict['class']._init_source
        kwargs = {'kwargs': {}, **task_dict}
        source = call_with_dictionary(fn, kwargs=kwargs)

        if extract_prod:
            task_dict['product'] = source.extract_product()

        task, up = task_dict.to_task(dag, root_path)

        if extract_prod:
            logger.debug('Extracted product for task "%s": %s', task.name,
                         task.product)

        upstream[task] = up
        source_obj[task] = source

    # second optional pass: extract upstream
    if extract_up:
        for task in list(dag.values()):
            upstream[task] = task.source.extract_upstream()
            logger.debug('Extracted upstream dependencies for task %s: %s',
                         task.name, upstream[task])

    # Last pass: set upstream dependencies
    for task in list(dag.values()):
        if upstream[task]:
            for task_name in upstream[task]:
                task.set_upstream(dag[task_name])


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
