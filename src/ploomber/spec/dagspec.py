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


Partials [Provisional name]
---------------------------
Partials were introduced to support a common ML use case: train and serving
pipelines. If we describe a training pipeline as a DAG, the only difference
with its serving counterpart are the tasks (can be more than one) at the
beginning (train: get historical data, serve: get single data point) and the
end (train: fit a model, serve: load a model and predict). Everything that
happens in the middle should be the same to avoid training-serving skew. There
are two specific use cases: batch and online.

To define a batch serving pipeline we define three files: pipeline.yaml
(train DAG), pipeline-serve (serve DAG), and pipeline-features.yaml (partial).
The first two follow the DAGSpec schema and use
``import_tasks_from: pipeline-features.yaml``. Training is done via
``ploomber build --entry-point pipeline.yaml`` and serving via
``ploomber build --entry-point pipeline-serve.yaml``.

An online API has a different mechanism because the inference pipeline needs
a Python API to interface with a web framework, rpc, or similar. This logic
is implemented by OnlineDAG, which takes a partial definition
(e.g. ``pipeline-features.yaml``) and builds an in-memory DAG that can make
predictions using ``OnlineDAG().predict()``. See ``OnlineDAG`` documentation
for details.
"""
import fnmatch
import os
import yaml
import logging
from pathlib import Path
from collections.abc import MutableMapping
from glob import iglob
from itertools import chain
import pprint

from ploomber.dag.DAG import DAG
from ploomber.placeholders.SourceLoader import SourceLoader
from ploomber.util.util import call_with_dictionary, add_to_sys_path
from ploomber.util import dotted_path
from ploomber.util.default import entry_point
from ploomber.spec.TaskSpec import TaskSpec, suffix2taskclass
from ploomber.util import validate
from ploomber.util import default
from ploomber.dag.DAGConfiguration import DAGConfiguration
from ploomber.exceptions import DAGSpecInitializationError
from ploomber.env.EnvDict import EnvDict
from ploomber.env.expand import expand_raw_dictionary, expand_raw_dictionaries
from ploomber.tasks import NotebookRunner
from ploomber.tasks.taskgroup import TaskGroup
from ploomber.validators.string import (validate_product_class_name,
                                        validate_task_class_name)
from ploomber.executors import Parallel

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
        an env.yaml file is loaded (in the current working diirectory), if
        it doesn't exist, it is loaded from the YAML spec parent folder, if it
        exists. If none of these exist, no env is loaded.

    lazy_import : bool, optional
        Whether to import dotted paths to initialize PythonCallables with the
        actual function. If False, PythonCallables are initialized directly
        with the dotted path, which means some verifications such as import
        statements in that function's module are delayed until the pipeline
        is executed. This also applies to placeholders loaded using a
        SourceLoader, if a placeholder exists, it will return the path
        to it, instead of an initialized Placeholder object, if it doesn't,
        it will return None instead of raising an error.

    reload : bool, optional
        Reloads modules before getting dotted paths. Has no effect if
        lazy_import=True
    """

    # NOTE: lazy_import is used where we need to initialized a a spec but don't
    # plan on running it. One use case is when exporting to Argo or Airflow:
    # we don't want to raise errors if some dependency is missing because
    # it can happen that the environment exporting the dag does not have
    # all the dependencies required to run it. The second use case is when
    # running "ploomber scaffold", we want to use DAGSpec machinery to parse
    # the yaml spec and the use such information to add the task in the
    # appropriate place, this by construction, means that the spec as it is
    # cannot be converted to a dag yet, since it has at least one task
    # whose source does not exist
    def __init__(self,
                 data,
                 env=None,
                 lazy_import=False,
                 reload=False,
                 parent_path=None):
        # initialized with a path to a yaml file...
        if isinstance(data, (str, Path)):
            if parent_path is not None:
                raise ValueError('parent_path must be None when '
                                 f'initializing {type(self).__name__} with '
                                 'a path to a YAML spec')
            # this is only used to display an error message with the path
            # to the loaded file
            path_for_errors = data
            # resolve the parent path to make sources and products unambiguous
            # even if the current working directory changes
            path_to_entry_point = Path(data).resolve()

            self._parent_path = str(path_to_entry_point.parent)

            content = Path(data).read_text()

            try:
                data = yaml.safe_load(content)
            except (yaml.parser.ParserError,
                    yaml.constructor.ConstructorError) as e:
                error = e
            else:
                error = None

            if error:
                if '{{' in content or '}}' in content:
                    raise DAGSpecInitializationError(
                        'Failed to initialize spec. It looks like '
                        'you\'re using placeholders (i.e. {{placeholder}}). '
                        'Make sure values are enclosed in parentheses '
                        '(e.g. key: "{{placeholder}}"). Original '
                        'parser error:\n\n'
                        f'{error}')
                else:
                    raise error

        # initialized with a dictionary...
        else:
            path_for_errors = None
            # FIXME: add test cases, some of those features wont work if
            # _parent_path is None. We should make sure that we either raise
            # an error if _parent_path is needed or use the current working
            # directory if it's appropriate - this is mostly to make relative
            # paths consistent: they should be relative to the file that
            # contains them
            self._parent_path = (None if not parent_path else str(
                Path(parent_path).resolve()))

        # try to look env.yaml in default locations
        env_default_path = default.path_to_env(self._parent_path)

        self.data = data

        if isinstance(self.data, list):
            self.data = {'tasks': self.data}

        # validate keys defined at the top (nested keys are not validated here)
        self._validate_top_keys(self.data, path_for_errors)

        logger.debug('DAGSpec enviroment:\n%s', pp.pformat(env))

        env = env or dict()

        # NOTE: when loading from a path, EnvDict recursively looks
        # at parent folders, this is useful when loading envs
        # in nested directories where scripts/functions need the env
        # but here, since we just need this for the spec, we might
        # want to turn it off. should we add a parameter to EnvDict
        # to control this?
        if env_default_path:
            defaults = yaml.safe_load(Path(env_default_path).read_text())
            self.env = EnvDict(env,
                               path_to_here=self._parent_path,
                               defaults=defaults)
        else:
            self.env = EnvDict(env, path_to_here=self._parent_path)

        self.data = expand_raw_dictionary(self.data, self.env)

        logger.debug('Expanded DAGSpec:\n%s', pp.pformat(data))

        # if there is a "location" top key, we don't have to do anything else
        # as we will just load the dotted path when .to_dag() is called
        if 'location' not in self.data:

            Meta.initialize_inplace(self.data)

            import_tasks_from = self.data['meta']['import_tasks_from']

            if import_tasks_from is not None:
                # when using a relative path in "import_tasks_from", we must
                # make it absolute...
                if not Path(import_tasks_from).is_absolute():
                    # use _parent_path if there is one
                    if self._parent_path:
                        self.data['meta']['import_tasks_from'] = str(
                            Path(self._parent_path, import_tasks_from))
                    # otherwise just make it absolute
                    else:
                        self.data['meta']['import_tasks_from'] = str(
                            Path(import_tasks_from).resolve())

                imported = yaml.safe_load(
                    Path(self.data['meta']['import_tasks_from']).read_text())

                if self.env is not None:
                    imported = expand_raw_dictionaries(imported, self.env)

                # relative paths here are relative to the file where they
                # are declared
                base_path = Path(self.data['meta']['import_tasks_from']).parent

                for task in imported:
                    add_base_path_to_source_if_relative(task,
                                                        base_path=base_path)

                self.data['tasks'].extend(imported)

            self.data['tasks'] = [
                normalize_task(task) for task in self.data['tasks']
            ]

            # "products" are relative to the project root. if no project
            # root, then use the parent path
            project_root = (default.find_root_recursively()
                            or self._parent_path)

            # make sure the folder where the pipeline is located is in sys.path
            # otherwise dynamic imports needed by TaskSpec will fail
            with add_to_sys_path(self._parent_path, chdir=False):
                self.data['tasks'] = [
                    TaskSpec(t,
                             self.data['meta'],
                             project_root=project_root,
                             lazy_import=lazy_import,
                             reload=reload) for t in self.data['tasks']
                ]
        else:
            self.data['meta'] = Meta.empty()

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
            valid = {
                'meta',
                'config',
                'clients',
                'tasks',
                'serializer',
                'unserializer',
                'executor',
            }
            validate.keys(valid, spec.keys(), name='dag spec')

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
            return dotted_path.call_dotted_path(self['location'])

        dag = DAG()

        if 'config' in self:
            dag._params = DAGConfiguration.from_dict(self['config'])

        if 'executor' in self:
            valid = {'serial', 'parallel'}
            executor = self['executor']

            if executor not in valid:
                raise ValueError('executor must be one '
                                 f'of {valid}, got: {executor}')

            if executor == 'parallel':
                dag.executor = Parallel()

        clients = self.get('clients')

        if clients:
            for class_name, dotted_path_spec in clients.items():
                dag.clients[class_name] = dotted_path.call_spec(
                    dotted_path_spec)

        # FIXME: this violates lazy_import, we must change DAG's implementation
        # to accept strings as attribute and load them until they are called
        for attr in ['serializer', 'unserializer']:
            if attr in self:
                setattr(dag, attr, dotted_path.load_dotted_path(self[attr]))

        process_tasks(dag, self, root_path=self._parent_path)

        return dag

    @classmethod
    def _auto_load(cls,
                   to_dag=True,
                   starting_dir=None,
                   env=None,
                   lazy_import=False,
                   reload=False):
        """
        NOTE: this is a private API. Use DAGSpec.find() instead

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
        root_path = starting_dir or os.getcwd()
        rel_path = entry_point(root_path=root_path)

        if rel_path is None:
            if to_dag:
                return None, None, None
            else:
                return None, None

        path = Path(root_path, rel_path)

        try:
            spec = cls(path, env=env, lazy_import=lazy_import, reload=reload)

            if to_dag:
                return spec, spec.to_dag(), Path(path).parent
            else:
                return spec, Path(path).parent

        except Exception as e:
            exc = DAGSpecInitializationError('Error initializing DAG from '
                                             'pipeline.yaml')
            raise exc from e

    @classmethod
    def find(cls, env=None, reload=False, lazy_import=False):
        """
        Automatically find pipeline.yaml and return a DAGSpec object, which
        can be converted to a DAG using .to_dag()

        Parameters
        ----------
        env
            The environment to pass to the spec
        """
        spec, _ = DAGSpec._auto_load(to_dag=False,
                                     starting_dir=None,
                                     env=env,
                                     lazy_import=lazy_import,
                                     reload=reload)
        return spec

    @classmethod
    def from_directory(cls, path_to_dir):
        """
        Construct a DAGSpec from a directory. Product and upstream are
        extracted from sources

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
        """
        Construct DAGSpec from list of files or glob-like pattern. Product and
        upstream are extracted from sources

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

        tasks = [{'source': file_} for file_ in files]
        meta = {'extract_product': True, 'extract_upstream': True}
        return cls({'tasks': tasks, 'meta': meta})


class Meta:
    """Schema for meta section in pipeline.yaml
    """
    VALID = {
        'extract_upstream',
        'extract_product',
        'product_default_class',
        'product_relative_to_source',
        'jupyter_hot_reload',
        'source_loader',
        'jupyter_functions_as_notebooks',
        'import_tasks_from',
    }

    @classmethod
    def initialize_inplace(cls, data):
        """Validate and instantiate the "meta" section
        """
        if 'meta' not in data:
            data['meta'] = {}

        data['meta'] = Meta.default_meta(data['meta'])

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
            meta['extract_product'] = False

        if 'product_relative_to_source' not in meta:
            meta['product_relative_to_source'] = False

        if 'jupyter_hot_reload' not in meta:
            meta['jupyter_hot_reload'] = False

        if 'jupyter_functions_as_notebooks' not in meta:
            meta['jupyter_functions_as_notebooks'] = False

        if 'import_tasks_from' not in meta:
            meta['import_tasks_from'] = None

        if 'source_loader' not in meta:
            meta['source_loader'] = None
        else:
            try:
                meta['source_loader'] = SourceLoader(**meta['source_loader'])
            except Exception as e:
                msg = ('Error initializing SourceLoader with '
                       f'{meta["source_loader"]}. Error message: {e.args[0]}')
                e.args = (msg, )
                raise

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

        # validate keys and values in product_default_class
        for task_name, product_name in meta['product_default_class'].items():
            try:
                validate_task_class_name(task_name)
                validate_product_class_name(product_name)
            except Exception as e:
                msg = f'Error validating product_default_class: {e.args[0]}'
                e.args = (msg, )
                raise

        return meta

    @classmethod
    def empty(cls):
        """Default meta values when a spec is initialized with "location"
        """
        return {v: None for v in cls.VALID}


def process_tasks(dag, dag_spec, root_path=None):
    """
    Initialize Task objects from TaskSpec, extract product and dependencies
    if needed and set the dag dependencies structure
    """
    root_path = root_path or '.'

    # options
    extract_up = dag_spec['meta']['extract_upstream']
    extract_prod = dag_spec['meta']['extract_product']

    # raw values extracted from the upstream key
    upstream_raw = {}

    # first pass: init tasks and them to dag
    for task_dict in dag_spec['tasks']:
        # init source to extract product
        fn = task_dict['class']._init_source
        kwargs = {'kwargs': {}, **task_dict}
        source = call_with_dictionary(fn, kwargs=kwargs)

        if extract_prod:
            task_dict['product'] = source.extract_product()

        # convert to task, up has the content of "upstream" if any
        task, up = task_dict.to_task(dag)

        if isinstance(task, TaskGroup):
            for t in task:
                upstream_raw[t] = up
        else:
            if extract_prod:
                logger.debug('Extracted product for task "%s": %s', task.name,
                             task.product)
            upstream_raw[task] = up

    # second optional pass: extract upstream
    tasks = list(dag.values())
    task_names = list(dag._iter())
    # actual upstream values after matching wildcards
    upstream = {}

    # expand upstream dependencies (in case there are any wildcards)
    for task in tasks:
        if extract_up:
            upstream[task] = _expand_upstream(task.source.extract_upstream(),
                                              task_names)
        else:
            upstream[task] = _expand_upstream(upstream_raw[task], task_names)

        logger.debug('Extracted upstream dependencies for task %s: %s',
                     task.name, upstream[task])

    # Last pass: set upstream dependencies
    for task in tasks:
        if upstream[task]:
            for task_name, group_name in upstream[task].items():
                try:
                    up = dag[task_name]
                except KeyError:
                    names = [t.name for t in tasks]
                    raise KeyError('Error processing spec. Extracted '
                                   'a reference to a task with name '
                                   f'{task_name!r}, but a task with such name '
                                   f'doesn\'t exist. Loaded tasks: {names}')

                task.set_upstream(up, group_name=group_name)


def normalize_task(task):
    # NOTE: we haven't documented that we support tasks as just strings,
    # should we keep this or deprecate it?
    if isinstance(task, str):
        return {'source': task}
    else:
        return task


def add_base_path_to_source_if_relative(task, base_path):
    path = Path(task['source'])
    relative_source = not path.is_absolute()

    # must be a relative source with a valid extension, otherwise, it can
    # be a dotted path
    if relative_source and path.suffix in set(suffix2taskclass):
        task['source'] = str(Path(base_path, task['source']).resolve())


def _expand_upstream(upstream, task_names):
    """
    Processes a list of upstream values extracted from source (or declared in
    the spec's "upstream" key). Expands wildcards like "some-task-*" to all
    the values that match. Returns a dictionary where keys are the upstream
    dependencies and the corresponding value is the wildcard. If no wildcard,
    the value is None
    """
    if not upstream:
        return None

    expanded = {}

    for up in upstream:
        if '*' in up:
            matches = fnmatch.filter(task_names, up)
            expanded.update({match: up for match in matches})
        else:
            expanded[up] = None

    return expanded
