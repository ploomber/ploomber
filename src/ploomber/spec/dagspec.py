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
import json
import os
import yaml
import logging
from pathlib import Path
from collections.abc import MutableMapping, Mapping
from glob import iglob
from itertools import chain
import pprint
import warnings

from ploomber.dag.dag import DAG
from ploomber.placeholders.sourceloader import SourceLoader
from ploomber.util.util import call_with_dictionary, add_to_sys_path
from ploomber.util import dotted_path
from ploomber.spec.taskspec import TaskSpec, suffix2taskclass
from ploomber.util import validate
from ploomber.util import default
from ploomber.dag.dagconfiguration import DAGConfiguration
from ploomber.exceptions import DAGSpecInitializationError, MissingKeysValidationError
from ploomber.env.envdict import EnvDict
from ploomber.env.expand import (
    expand_raw_dictionary_and_extract_tags,
    expand_raw_dictionaries_and_extract_tags,
)
from ploomber.tasks import NotebookRunner
from ploomber.tasks.taskgroup import TaskGroup
from ploomber.validators.string import (
    validate_product_class_name,
    validate_task_class_name,
)
from ploomber.executors import Parallel
from ploomber.io import pretty_print

logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)


class DAGSpec(MutableMapping):
    """
    A DAG spec is a dictionary with certain structure that can be converted
    to a DAG using ``DAGSpec.to_dag()``.

    There are two cases: the simplest one is just a dictionary with a
    "location" key with the factory to call, the other explicitly describes
    the DAG structure as a dictionary.

    When ``.to_dag()`` is called, the current working directory is temporarily
    switched to the spec file parent folder (only applies when loading from
    a file)

    Parameters
    ----------
    data : str, pathlib.Path or dict
        Path to a YAML spec or dict spec. If loading from a file, sources
        and products are resolved to the file's parent. If the file is in a
        packaged structure (i.e., src/package/pipeline.yaml), the existence
        of a setup.py in the same folder as src/ is validated. If loaded from
        a dict, sources and products aren't resolved, unless a parent_path
        is not None.

    env : dict, pathlib.path or str, optional
        If path it must be a YAML file. Environment to load. Any string with
        the format '{{placeholder}}' in the spec is replaced by the
        corresponding value in the given key (i.e., placeholder). If ``env``
        is None and spec is a dict, no env is loaded. If None and loaded
        from a YAML file, an ``env.yaml`` file is loaded from the current
        working diirectory, if it doesn't exist, it is loaded from the YAML
        spec parent folder. If none of these exist, no env is loaded.
        A :py:mod:`ploomber.Env` object is initialized, see documentation for
        details.

    lazy_import : bool, optional
        Whether to import dotted paths to initialize PythonCallables with the
        actual function. If False, PythonCallables are initialized directly
        with the dotted path, which means some verifications such as import
        statements in that function's module are delayed until the pipeline
        is executed. This also applies to placeholders loaded using a
        SourceLoader, if a template exists, it will return the path
        to it, instead of initializing it, if it doesn't, it will return None
        instead of raising an error. This setting is useful when we require to
        load YAML spec and instantiate the DAG object to extract information
        from it (e.g., which are the declared tasks) but the process running
        it may not have all the required dependencies to do so (e.g., an
        imported library in a PythonCallable task).

    reload : bool, optional
        Reloads modules before importing dotted paths to detect code changes
        if the module has already been imported. Has no effect if
        lazy_import=True.

    Examples
    --------
    Load from ``pipeline.yaml``:

    >>> from ploomber.spec import DAGSpec
    >>> spec = DAGSpec('spec/pipeline.yaml') # load spec
    >>> dag = spec.to_dag() # convert to DAG
    >>> status = dag.status()

    Override ``env.yaml``:

    >>> from ploomber.spec import DAGSpec
    >>> spec = DAGSpec('spec/pipeline.yaml', env=dict(key='value'))
    >>> dag = spec.to_dag()
    >>> status = dag.status()


    See Also
    --------
    ploomber.DAG
        Pipeline internal representation, implements the methods in the
        command-line interface (e.g., ``DAG.build()``, or ``DAG.plot``)


    Attributes
    ----------
    path : str or None
        Returns the path used to load the data. None if loaded from a
        dictionary
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
    def __init__(
        self, data, env=None, lazy_import=False, reload=False, parent_path=None
    ):
        self._init(
            data=data,
            env=env,
            lazy_import=lazy_import,
            reload=reload,
            parent_path=parent_path,
            look_up_project_root_recursively=True,
        )

    def _init(
        self,
        data,
        env,
        lazy_import,
        reload,
        parent_path,
        look_up_project_root_recursively,
    ):
        self._lazy_import = lazy_import
        self._name = None
        data_argument_is_filepath = isinstance(data, (str, Path))

        # initialized with a path to a yaml file...
        if data_argument_is_filepath:
            # TODO: test this
            if parent_path is not None:
                raise ValueError(
                    "parent_path must be None when "
                    f"initializing {type(self).__name__} with "
                    "a path to a YAML spec"
                )
            # resolve the parent path to make sources and products unambiguous
            # even if the current working directory changes
            self._path = Path(data).resolve()
            self._parent_path = str(self._path.parent)

            # assign the name of the parent directory
            self._name = self._path.parent.name

            if not Path(data).is_file():
                raise FileNotFoundError(
                    "Error initializing DAGSpec with argument "
                    f"{data!r}: Expected it to be a path to a YAML file, but "
                    "such file does not exist"
                )

            content = Path(data).read_text()

            try:
                data = yaml.safe_load(content)
            except (
                yaml.parser.ParserError,
                yaml.constructor.ConstructorError,
                yaml.scanner.ScannerError,
            ) as e:
                error = e
            else:
                error = None

            if error:
                if "{{" in content or "}}" in content:
                    raise DAGSpecInitializationError(
                        "Failed to initialize spec. It looks like "
                        "you're using placeholders (i.e. {{placeholder}}). "
                        "Make sure values are enclosed in parentheses "
                        '(e.g. key: "{{placeholder}}"). Original '
                        "parser error:\n\n"
                        f"{error}"
                    )
                else:
                    raise DAGSpecInitializationError(
                        "Failed to initialize spec. Got invalid YAML"
                    ) from error

        # initialized with a dictionary...
        else:
            self._path = None
            # FIXME: add test cases, some of those features wont work if
            # _parent_path is None. We should make sure that we either raise
            # an error if _parent_path is needed or use the current working
            # directory if it's appropriate - this is mostly to make relative
            # paths consistent: they should be relative to the file that
            # contains them
            self._parent_path = (
                None if not parent_path else str(Path(parent_path).resolve())
            )

        self.data = data

        if isinstance(self.data, list):
            self.data = {"tasks": self.data}

        if self.data.get("tasks") and not isinstance(self.data["tasks"], list):
            raise DAGSpecInitializationError(
                "Expected 'tasks' in the dag spec to contain a "
                f'list, but got: {self.data["tasks"]} '
                "(an object with "
                f'type: {type(self.data["tasks"]).__name__!r})'
            )

        # validate keys defined at the top (nested keys are not validated here)
        self._validate_top_keys(self.data, self._path)

        logger.debug("DAGSpec enviroment:\n%s", pp.pformat(env))

        env = env or dict()
        path_to_defaults = default.path_to_env_from_spec(path_to_spec=self._path)
        # there is an env.yaml we can use
        if path_to_defaults:
            defaults = yaml.safe_load(Path(path_to_defaults).read_text())
            self.env = EnvDict(env, path_to_here=self._parent_path, defaults=defaults)

        # there is no env.yaml
        else:
            # wrong extension of env.yml or env.{name}.yml found
            if default.try_to_find_env_yml(path_to_spec=self._path):
                raise DAGSpecInitializationError(
                    "Error: found env file ends with .yml. "
                    "Change the extension to .yaml."
                )
            self.env = EnvDict(env, path_to_here=self._parent_path)

        self.data, tags = expand_raw_dictionary_and_extract_tags(self.data, self.env)

        logger.debug("Expanded DAGSpec:\n%s", pp.pformat(data))

        # if there is a "location" top key, we don't have to do anything else
        # as we will just load the dotted path when .to_dag() is called
        if "location" not in self.data:
            Meta.initialize_inplace(self.data)

            import_tasks_from = self.data["meta"]["import_tasks_from"]

            if import_tasks_from is not None:
                # when using a relative path in "import_tasks_from", we must
                # make it absolute...
                if not Path(import_tasks_from).is_absolute():
                    # use _parent_path if there is one
                    if self._parent_path:
                        self.data["meta"]["import_tasks_from"] = str(
                            Path(self._parent_path, import_tasks_from)
                        )
                    # otherwise just make it absolute
                    else:
                        self.data["meta"]["import_tasks_from"] = str(
                            Path(import_tasks_from).resolve()
                        )

                imported = yaml.safe_load(
                    Path(self.data["meta"]["import_tasks_from"]).read_text()
                )

                if not imported:
                    path = str(self.data["meta"]["import_tasks_from"])
                    raise ValueError(
                        "expected import_tasks_from file "
                        f"({path!r}) to return a list of tasks, "
                        f"got: {imported}"
                    )

                if not isinstance(imported, list):
                    raise TypeError(
                        "Expected list when loading YAML file from "
                        "import_tasks_from: file.yaml, "
                        f"but got {type(imported)}"
                    )

                if self.env is not None:
                    (imported, tags_other) = expand_raw_dictionaries_and_extract_tags(
                        imported, self.env
                    )
                    tags = tags | tags_other

                # relative paths here are relative to the file where they
                # are declared
                base_path = Path(self.data["meta"]["import_tasks_from"]).parent

                for task in imported:
                    add_base_path_to_source_if_relative(task, base_path=base_path)

                self.data["tasks"].extend(imported)

            # check if there are any params declared in env, not used in
            # in the pipeline
            extra = self.env.get_unused_placeholders() - tags

            if extra:
                warnings.warn(
                    "The following placeholders are declared in the "
                    "environment but "
                    f"unused in the spec: {extra}"
                )

            if self.data["tasks"] is None:
                raise DAGSpecInitializationError(
                    'Failed to initialize spec, "tasks" section is empty'
                )

            self.data["tasks"] = [normalize_task(task) for task in self.data["tasks"]]

            # NOTE: for simple projects, project root is the parent folder
            # of pipeline.yaml, for package projects is the parent folder
            # of setup.py
            if look_up_project_root_recursively:
                project_root = (
                    None
                    if not self._parent_path
                    else default.find_root_recursively(
                        starting_dir=self._parent_path,
                        filename=None if not self._path else self._path.name,
                    )
                )
            else:
                project_root = self._parent_path

            # make sure the folder where the pipeline is located is in sys.path
            # otherwise dynamic imports needed by TaskSpec will fail
            with add_to_sys_path(self._parent_path, chdir=False):
                task_specs = []
                for t in self.data["tasks"]:
                    try:
                        task_specs.append(
                            TaskSpec(
                                t,
                                self.data["meta"],
                                project_root=project_root,
                                lazy_import=lazy_import,
                                reload=reload,
                            )
                        )
                    except MissingKeysValidationError as e:
                        example_spec = _build_example_spec(t["source"])
                        if data_argument_is_filepath:
                            example_str = yaml.dump(example_spec, sort_keys=False)
                        else:
                            example_str = json.dumps(example_spec)
                        e.message += (
                            "\nTo fix it, add the missing key " + "(example):\n\n{}"
                        ).format(example_str)
                        raise e
                self.data["tasks"] = task_specs
        else:
            self.data["meta"] = Meta.empty()

    def _validate_top_keys(self, spec, path):
        """Validate keys at the top of the spec"""
        if "tasks" not in spec and "location" not in spec:
            raise DAGSpecInitializationError(
                'Failed to initialize spec. Missing "tasks" key'
            )

        if "location" in spec:
            if len(spec) > 1:
                raise DAGSpecInitializationError(
                    "Failed to initialize spec. If "
                    'using the "location" key there should not '
                    "be other keys"
                )

        else:
            valid = {
                "meta",
                "config",
                "clients",
                "tasks",
                "serializer",
                "unserializer",
                "executor",
                "on_finish",
                "on_render",
                "on_failure",
            }
            validate.keys(valid, spec.keys(), name="dag spec")

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
        """Converts the DAG spec to a DAG object"""
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
        if "location" in self:
            return dotted_path.call_dotted_path(self["location"])

        dag = DAG(name=self._name)

        if "config" in self:
            dag._params = DAGConfiguration.from_dict(self["config"])

        if "executor" in self:
            executor = self["executor"]

            if isinstance(executor, str) and executor in {"serial", "parallel"}:
                if executor == "parallel":
                    dag.executor = Parallel()
            elif isinstance(executor, Mapping):
                dag.executor = dotted_path.DottedPath(
                    executor, lazy_load=False, allow_return_none=False
                )()
            else:
                raise DAGSpecInitializationError(
                    '"executor" must be '
                    '"serial", "parallel", or a dotted path'
                    f", got: {executor!r}"
                )

        clients = self.get("clients")

        if clients:
            for class_name, dotted_path_spec in clients.items():
                if dotted_path_spec is None:
                    continue
                dps = dotted_path.DottedPath(
                    dotted_path_spec,
                    lazy_load=self._lazy_import,
                    allow_return_none=False,
                )

                if self._lazy_import:
                    dag.clients[class_name] = dps
                else:
                    dag.clients[class_name] = dps()

        for attr in (
            "serializer",
            "unserializer",
            "on_finish",
            "on_render",
            "on_failure",
        ):
            if attr in self:
                setattr(
                    dag,
                    attr,
                    dotted_path.DottedPath(self[attr], lazy_load=self._lazy_import),
                )

        process_tasks(dag, self, root_path=self._parent_path)

        return dag

    @property
    def path(self):
        return self._path

    @classmethod
    def _find_relative(cls, name=None, lazy_import=False):
        """
        Searches for a spec in default locations relative to the current
        working directory

        Notes
        -----
        This is a private API used by Soopervisor to locate which YAML spec
        to use for exporting. It needs a relative path since the location
        is used for loading the DAG and submitting tasks and as an argument
        in "ploomber task --entry-point {relative-path}" when executing tasks
        """
        relative_path = default.entry_point_relative(name=name)
        return cls(relative_path, lazy_import=lazy_import), relative_path

    @classmethod
    def find(
        cls, env=None, reload=False, lazy_import=False, starting_dir=None, name=None
    ):
        """
        Automatically find pipeline.yaml and return a DAGSpec object, which
        can be converted to a DAG using .to_dag()

        Parameters
        ----------
        env
            The environment to pass to the spec

        name : str, default=None
            Filename to search for. If None, it looks for a pipeline.yaml file,
            otherwise it looks for a file with such name.
        """
        starting_dir = starting_dir or os.getcwd()
        path_to_entry_point = default.entry_point_with_name(
            root_path=starting_dir, name=name
        )

        try:
            return cls(
                path_to_entry_point, env=env, lazy_import=lazy_import, reload=reload
            )
        except Exception as e:
            exc = DAGSpecInitializationError(
                "Error initializing DAG from " f"{path_to_entry_point!s}"
            )
            raise exc from e

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
            name
            for name, class_ in suffix2taskclass.items()
            if class_ is NotebookRunner
        ]

        if Path(path_to_dir).is_dir():
            pattern = str(Path(path_to_dir, "*"))
            files = list(
                chain.from_iterable(iglob(pattern + ext) for ext in valid_extensions)
            )
            return cls.from_files(files)
        else:
            raise NotADirectoryError(f"{path_to_dir!r} is not a directory")

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
            name
            for name, class_ in suffix2taskclass.items()
            if class_ is NotebookRunner
        ]

        if isinstance(files, str):
            files = [f for f in iglob(files) if Path(f).is_file()]

        invalid = [f for f in files if Path(f).suffix not in valid_extensions]

        if invalid:
            raise ValueError(
                f"Cannot instantiate DAGSpec from files with "
                f"invalid extensions: {invalid}. "
                f"Allowed extensions are: {valid_extensions}"
            )

        tasks = [{"source": file_} for file_ in files]
        meta = {"extract_product": True, "extract_upstream": True}
        return cls({"tasks": tasks, "meta": meta})


class DAGSpecPartial(DAGSpec):
    """
    A DAGSpec subclass that initializes from a list of tasks (used in the
    onlinedag.py) module
    """

    def __init__(self, path_to_partial, env=None):
        with open(path_to_partial) as f:
            tasks = yaml.safe_load(f)

        if not isinstance(tasks, list):
            raise ValueError(
                f"Expected partial {path_to_partial!r} to be a "
                f"list of tasks, but got a {type(tasks).__name__} instead"
            )

        # cannot extract upstream because this is an incomplete DAG
        meta = {"extract_product": False, "extract_upstream": False}
        data = {"tasks": tasks, "meta": meta}

        env = env or default.path_to_env_from_spec(path_to_partial)

        self._init(
            data=data,
            env=env,
            lazy_import=False,
            reload=False,
            parent_path=Path(path_to_partial).parent,
            look_up_project_root_recursively=False,
        )


class Meta:
    """Schema for meta section in pipeline.yaml"""

    VALID = {
        "extract_upstream",
        "extract_product",
        "product_default_class",
        "product_relative_to_source",
        "jupyter_hot_reload",
        "source_loader",
        "jupyter_functions_as_notebooks",
        "import_tasks_from",
    }

    @classmethod
    def initialize_inplace(cls, data):
        """Validate and instantiate the "meta" section"""
        if "meta" not in data:
            data["meta"] = {}

        data["meta"] = Meta.default_meta(data["meta"])

    @classmethod
    def default_meta(cls, meta=None):
        """Fill missing values in a meta dictionary"""
        if meta is None:
            meta = {}

        validate.keys(cls.VALID, meta, name="dag spec")

        if "extract_upstream" not in meta:
            meta["extract_upstream"] = True

        if "extract_product" not in meta:
            meta["extract_product"] = False

        if "product_relative_to_source" not in meta:
            meta["product_relative_to_source"] = False

        if "jupyter_hot_reload" not in meta:
            meta["jupyter_hot_reload"] = True

        if "jupyter_functions_as_notebooks" not in meta:
            meta["jupyter_functions_as_notebooks"] = False

        if "import_tasks_from" not in meta:
            meta["import_tasks_from"] = None

        if "source_loader" not in meta:
            meta["source_loader"] = None
        else:
            try:
                meta["source_loader"] = SourceLoader(**meta["source_loader"])
            except Exception as e:
                msg = (
                    "Error initializing SourceLoader with "
                    f'{meta["source_loader"]}. Error message: {e.args[0]}'
                )
                e.args = (msg,)
                raise

        defaults = {
            "SQLDump": "File",
            "NotebookRunner": "File",
            "ScriptRunner": "File",
            "SQLScript": "SQLRelation",
            "PythonCallable": "File",
            "ShellScript": "File",
        }

        if "product_default_class" not in meta:
            meta["product_default_class"] = defaults
        else:
            for class_, prod in defaults.items():
                if class_ not in meta["product_default_class"]:
                    meta["product_default_class"][class_] = prod

        # validate keys and values in product_default_class
        for task_name, product_name in meta["product_default_class"].items():
            try:
                validate_task_class_name(task_name)
                validate_product_class_name(product_name)
            except Exception as e:
                msg = f"Error validating product_default_class: {e.args[0]}"
                e.args = (msg,)
                raise

        return meta

    @classmethod
    def empty(cls):
        """Default meta values when a spec is initialized with "location" """
        return {v: None for v in cls.VALID}


def process_tasks(dag, dag_spec, root_path=None):
    """
    Initialize Task objects from TaskSpec, extract product and dependencies
    if needed and set the dag dependencies structure
    """
    root_path = root_path or "."

    # options
    extract_up = dag_spec["meta"]["extract_upstream"]
    extract_prod = dag_spec["meta"]["extract_product"]

    # raw values extracted from the upstream key
    upstream_raw = {}

    # first pass: init tasks and them to dag
    for task_dict in dag_spec["tasks"]:
        # init source to extract product
        fn = task_dict["class"]._init_source
        kwargs = {
            "kwargs": {},
            "extract_up": extract_up,
            "extract_prod": extract_prod,
            **task_dict,
        }
        source = call_with_dictionary(fn, kwargs=kwargs)

        if extract_prod:
            task_dict["product"] = source.extract_product()

        # convert to task, up has the content of "upstream" if any
        task, up = task_dict.to_task(dag)

        if isinstance(task, TaskGroup):
            for t in task:
                upstream_raw[t] = up
        else:
            if extract_prod:
                logger.debug(
                    'Extracted product for task "%s": %s', task.name, task.product
                )
            upstream_raw[task] = up

    # second optional pass: extract upstream
    tasks = list(dag.values())
    task_names = list(dag._iter())
    # actual upstream values after matching wildcards
    upstream = {}

    # expand upstream dependencies (in case there are any wildcards)
    for task in tasks:
        if extract_up:
            try:
                extracted = task.source.extract_upstream()
            except Exception as e:
                raise DAGSpecInitializationError(
                    f"Failed to initialize task {task.name!r}"
                ) from e

            upstream[task] = _expand_upstream(extracted, task_names)
        else:
            upstream[task] = _expand_upstream(upstream_raw[task], task_names)

        logger.debug(
            "Extracted upstream dependencies for task %s: %s", task.name, upstream[task]
        )

    # Last pass: set upstream dependencies
    for task in tasks:
        if upstream[task]:
            for task_name, group_name in upstream[task].items():
                up = dag.get(task_name)

                if up is None:
                    names = [t.name for t in tasks]
                    raise DAGSpecInitializationError(
                        f"Task {task.name!r} "
                        "has an upstream dependency "
                        f"{task_name!r}, but such task "
                        "doesn't exist. Available tasks: "
                        f"{pretty_print.iterable(names)}"
                    )

                task.set_upstream(up, group_name=group_name)


def normalize_task(task):
    # NOTE: we haven't documented that we support tasks as just strings,
    # should we keep this or deprecate it?
    if isinstance(task, str):
        return {"source": task}
    else:
        return task


def add_base_path_to_source_if_relative(task, base_path):
    path = Path(task["source"])
    relative_source = not path.is_absolute()

    # must be a relative source with a valid extension, otherwise, it can
    # be a dotted path
    if relative_source and path.suffix in set(suffix2taskclass):
        task["source"] = str(Path(base_path, task["source"]).resolve())


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
        if "*" in up:
            matches = fnmatch.filter(task_names, up)
            expanded.update({match: up for match in matches})
        else:
            expanded[up] = None

    return expanded


def _build_example_spec(source):
    """
    Build an example spec from just the source.
    """
    example_spec = {}
    example_spec["source"] = source
    if suffix2taskclass[Path(source).suffix] is NotebookRunner:
        example_product = {
            "nb": "products/report.ipynb",
            "data": "products/data.csv",
        }
    else:
        example_product = "products/data.csv"
    example_spec = [
        {
            "source": source,
            "product": example_product,
        }
    ]
    return example_spec
