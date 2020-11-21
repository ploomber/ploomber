"""
Path management
---------------
Relative paths are heavily used for pointing to source files and products,
when only the Python API existed, this was unambiguous: all paths
were relative to the current working directory, you only had to make sure
to start the session at the right place. Initially, the Spec API didn't
change a thing since the assumption was that one would convert a spec to
a DAG in a folder containing a pipeline.yaml, under this scenario the
current directory is also the parent of pipeline.yaml so all relative paths
work as expected.

However, when we introduced the Jupyter custom contents manager things
changed because the DAG can be initialized from directories where there
isn't a pipeline.yaml but we have to recursively look for one. Furthermore,
if extract_upstream or extract_product are True and the source files do
not share the parent with pipeline.yaml an ambiguity arises: are paths
inside a source file relative to the source file, current directory or
pipeline.yaml?

When we started working on Soopervisor, the concept of "project" arised,
which is a folder that contains all necessary files to instantiate a
Ploomber pipeline. To take current directory ambiguity out of the situation
we decided that projects that use the Spec API should ignore the current
directory and assume that *all relative paths in the spec are so to the
pipeline.yaml parent folder* this means that we could initialize projects
from anywhere by just pointing to the project's root folder (the one that
contains pipeline.yaml). This is implemented in TaskSpec.py@init_product
where we add the project's root folder to all relative paths. This implies
that all relative paths are converted to absolute to make them agnostic
from the current working directory. The basic idea is that a person
writing a DAG spec should only have to know that paths are relative
to the project's root folder and the DAGSpec should do the heavy lifting
of instantiating a spec that could be converted to a dag (and reendered)
independent of the working directory, because the project's root folder
defines the pipelines's scope, and there is no point in looking for files
outside of it, except when asking explicity through an absolute path.

However, this behavior clashes with the Jupyter notebook defaults. Jupyter
sets the current working directory not to the directory where
"jupyter {notebook/lab}" was called but to the current file parent's.
Our current logic would override this default behavior so we added an
option to turn it back on (product_relative_to_source).

Note that this does not affect the Python API, since that would create more
confusion. The Python API should work as expected from any other library:
paths should be relative to the current working directory.

The only case where we can't determine a project's root is when a DAGSpec
is initialized from a dictionary directly, but this is not something
that end-users would do.

Spec sections
-------------
"meta" is for settings that cannot be clearly mapped to the OOP Python
interface, we don't call it config because there is a DAGConfig object in
the Python API and this might cause confusion

All other sections should represent valid DAG properties.
"""
import os
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
                                call_with_dictionary, add_to_sys_path)
from ploomber.spec.TaskSpec import TaskSpec, suffix2taskclass
from ploomber.util import validate
from ploomber.dag.DAGConfiguration import DAGConfiguration
from ploomber.exceptions import DAGSpecInitializationError
from ploomber.env.EnvDict import EnvDict
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

    When .to_dag() is called, the current working directory is temporarily
    switched to the spec file parent folder (only applies when loading from
    a file)

    Parameters
    ----------
    data : str, pathlib.Path or dict
        Path to a YAML spec or dict spec to construct the DAG

    env : dict, pathlib.path or str, optional
        Dictionary or path/str to a YAML file with the environment to use,
        tags in any of the keys (i.e. {{some_tag}}) in "data" will be replaced
        by the corresponding values in "env". A regular :py:mod:`ploomber.Env`
        object is created, see documentation for details. If None and
        data is a dict, no env is loaded. If None and loaded from a YAML spec,
        an env.yaml file is loaded from the YAML spec parent folder, if it
        exists

    lazy_import : bool, optional
        Whether to import dotted paths to initialize PythonCallables with the
        actual function. If False, PythonCallables are initialized directly
        with the dotted path, which means some verifications such as import
        statements in that function's module are delayed until the pipeline
        is executed
    """
    def __init__(self, data, env=None, lazy_import=False):
        if isinstance(data, (str, Path)):
            path = data
            # resolve the parent path to make sources and products unambiguous
            # even if the current working directory changes
            self._parent_path = str(Path(data).parent.resolve())

            with open(str(data)) as f:
                data = yaml.load(f, Loader=yaml.SafeLoader)

            env_path = Path(self._parent_path, 'env.yaml')

            if env is None and env_path.exists():
                env = str(env_path)
        else:
            path = None
            self._parent_path = None

        self.data = data

        if isinstance(self.data, list):
            self.data = data = {'tasks': self.data}

        # validate keys defined at the top (nested keys are not validated here)
        self._validate_top_keys(self.data, path)

        logger.debug('DAGSpec enviroment:\n%s', pp.pformat(env))

        if env is not None:
            # NOTE: when loading from a path, EnvDict recursively looks
            # at parent folders, this is useful when loading envs
            # in nested directories where scripts/functions need the env
            # but here, since we just need this for the spec, we might
            # want to turn it off. should we add a parameter to EnvDict
            # to control this?
            self.env = EnvDict(env)
            self.data = expand_raw_dictionary(self.data, self.env)
        else:
            self.env = None

        logger.debug('Expanded DAGSpec:\n%s', pp.pformat(data))

        # if there is a "location" top key, we don't have to do anything else
        # as we will just load the dotted path when .to_dag() is called
        if 'location' not in self.data:
            self.data['tasks'] = [
                normalize_task(task) for task in self.data['tasks']
            ]
            self._validate_meta()

            # make sure the folder where the pipeline is located is in sys.path
            # otherwise dynamic imports needed by TaskSpec will fail
            with add_to_sys_path(self._parent_path, chdir=False):
                self.data['tasks'] = [
                    TaskSpec(t,
                             self.data['meta'],
                             project_root=self._parent_path,
                             lazy_import=lazy_import)
                    for t in self.data['tasks']
                ]
        else:
            self.data['meta'] = Meta.default_meta_location()

    def _validate_top_keys(self, spec, path):
        """Validate keys at the top of the spec
        """
        if 'tasks' not in spec and 'location' not in spec:
            path_ = f'(file: "{path}")' if self._parent_path else ''
            raise KeyError('Invalid data to initialize DAGSpec, missing '
                           f'key "tasks" {path_}')

        if 'location' in spec:
            if len(spec) > 1:
                raise KeyError('If specifying dag through a "location" key '
                               'it must be the unique key in the spec')
        else:
            valid = {'meta', 'config', 'clients', 'tasks'}
            validate.keys(valid, spec.keys(), name='dag spec')

    def _validate_meta(self):
        """Validate and instantiate the "meta" section
        """
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
        # when initializing DAGs from pipeline.yaml files, we have to ensure
        # that the folder where pipeline.yaml is located is in sys.path for
        # imports to work (for dag clients), this happens most of the time but
        # for some (unknown) reason, it doesn't
        # happen when initializing PloomberContentsManager.
        # pipeline.yaml paths are written relative to that file, for source
        # scripts to be located we temporarily change the current working
        # directory
        with add_to_sys_path(self._parent_path, chdir=True):
            dag = self._to_dag()

        return dag

    def _to_dag(self):
        """
        Internal method to manage the different cases to convert to a DAG
        object
        """
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
    def auto_load(cls, to_dag=True, starting_dir=None, env=None):
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

        Returns DAG and the directory where the pipeline.yaml file is located.
        """
        path = find_file_recursively('pipeline.yaml',
                                     starting_dir=starting_dir or os.getcwd())

        if path is None:
            if to_dag:
                return None, None, None
            else:
                return None, None

        try:
            spec = cls(path, env=env)

            if to_dag:
                return spec, spec.to_dag(), Path(path).parent
            else:
                return spec, Path(path).parent

        except Exception as e:
            exc = DAGSpecInitializationError('Error initializing DAG from '
                                             'pipeline.yaml')
            raise exc from e

    @classmethod
    def from_directory(cls, path_to_dir):
        """Construct a DAGSpec from a directory

        Parameters
        ----------
        path_to_dir : str
            The directory to use.  Looks for scripts
            (``.py``, ``.R`` or ``.ipynb``) in the directory and interprets
            them as task sources, file names are assigned as task names
            (without extension). The spec is generated with the default values
            in the "meta" section. Ignores files with invalid extensions.

        Notes
        -----
        ``env`` is not supported because the spec is generated from files
        in ``path_to_dir``, hence, there is no way to embed tags
        """
        valid_extensions = [
            name for name, class_ in suffix2taskclass.items()
            if class_ is NotebookRunner
        ]

        if Path(path_to_dir).is_dir():
            pattern = str(Path(path_to_dir, '*'))
            files = list(
                chain.from_iterable(
                    iglob(pattern + ext) for ext in valid_extensions))
            return cls.from_files(files)
        else:
            raise NotADirectoryError(f'{path_to_dir!r} is not a directory')

    @classmethod
    def from_files(cls, files):
        """Construct DAGSpec from list of files or glob-like pattern

        Parameters
        ----------
        files : list or str
            List of files to use or glob-like string pattern. If glob-like
            pattern, ignores directories that match the criteria.
        """
        valid_extensions = [
            name for name, class_ in suffix2taskclass.items()
            if class_ is NotebookRunner
        ]

        if isinstance(files, str):
            files = [f for f in iglob(files) if Path(f).is_file()]

        invalid = [f for f in files if Path(f).suffix not in valid_extensions]

        if invalid:
            raise ValueError(f'Cannot instantiate DAGSpec from files with '
                             f'invalid extensions: {invalid}. '
                             f'Allowed extensions are: {valid_extensions}')

        tasks = [{
            'source': file_,
            'name': str(Path(file_).with_suffix('').name)
        } for file_ in files]

        return cls({'tasks': tasks})


class Meta:
    """Schema for meta section in pipeline.yaml
    """
    VALID = {
        'extract_upstream', 'extract_product', 'product_default_class',
        'product_relative_to_source', 'jupyter_hot_reload', 'source_loader'
    }

    @classmethod
    def default_meta(cls, meta=None):
        """Fill missing values in a meta dictionary
        """
        if meta is None:
            meta = {}

        validate.keys(cls.VALID, meta, name='dag spec')

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
            'SQLScript': 'SQLRelation',
            'PythonCallable': 'File',
            'ShellScript': 'File',
        }

        if 'product_default_class' not in meta:
            meta['product_default_class'] = defaults
        else:
            for class_, prod in defaults.items():
                if class_ not in meta['product_default_class']:
                    meta['product_default_class'][class_] = prod

        return meta

    @classmethod
    def default_meta_location(cls):
        """Default meta values when a spec is initialized with "location"
        """
        return {v: None for v in cls.VALID}


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

        task, up = task_dict.to_task(dag)

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
    # NOTE: we haven't documented that we support tasks as just strings,
    # should we keep this or deprecate it?
    if isinstance(task, str):
        return {'source': task}
    else:
        return task
