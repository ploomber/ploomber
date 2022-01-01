"""
Create Tasks from dictionaries
"""
from functools import partial
from copy import copy, deepcopy
from pathlib import Path
from collections.abc import MutableMapping, Mapping
import platform

from ploomber import tasks, products
from ploomber.util.util import _make_iterable
from ploomber.util import validate, dotted_path
from ploomber.tasks.taskgroup import TaskGroup
from ploomber import validators
from ploomber.exceptions import DAGSpecInitializationError
from ploomber.products._resources import resolve_resources
from ploomber.io import pretty_print

suffix2taskclass = {
    '.py': tasks.NotebookRunner,
    '.R': tasks.NotebookRunner,
    '.Rmd': tasks.NotebookRunner,
    '.r': tasks.NotebookRunner,
    '.ipynb': tasks.NotebookRunner,
    '.sql': tasks.SQLScript,
    '.sh': tasks.ShellScript
}


def _safe_suffix(product):
    try:
        return Path(product).suffix
    except Exception:
        return None


def _looks_like_path(s):
    system = platform.system()
    if system == 'Windows':
        return '\\' in s
    else:
        return '/' in s


def task_class_from_source_str(source_str, lazy_import, reload, product):
    """
    The source field in a DAG spec is a string. The actual value needed to
    instantiate the task depends on the task class, but to make task class
    optional, we try to guess the appropriate task here. If the source_str
    needs any pre-processing to pass it to the task constructor, it also
    happens here. If product is not None, it's also used to determine if
    a task is a SQLScript or SQLDump
    """
    extension = Path(source_str).suffix

    # we verify if this is a valid dotted path
    # if lazy load is set to true, just locate the module without importing it

    fn_checker = (
        dotted_path.locate_dotted_path_root if lazy_import is True else
        partial(dotted_path.load_dotted_path, raise_=True, reload=reload))

    if extension and extension in suffix2taskclass:
        if extension == '.sql' and _safe_suffix(product) in {
                '.csv', '.parquet'
        }:
            return tasks.SQLDump

        return suffix2taskclass[extension]
    elif _looks_like_path(source_str):
        raise ValueError('Failed to determine task class for '
                         f'source {source_str!r} (invalid '
                         f'extension {extension!r}). Valid extensions '
                         f'are: {pretty_print.iterable(suffix2taskclass)}')
    elif lazy_import == 'skip':
        # Anything that has not been caught before is treated as a
        # Python function, thus we return a PythonCallable
        return tasks.PythonCallable
    else:
        try:
            imported = fn_checker(source_str)
            error = None
        except Exception as e:
            imported = None
            error = e

        if imported is None:
            raise ValueError(
                'Could not determine task class for '
                f'source {source_str!r} due to error: {error!s}. '
                'This looks like a dotted path but it failed to import. '
                'You can also set the task class using the "class" key.')
        else:
            return tasks.PythonCallable


def task_class_from_spec(task_spec, lazy_import, reload):
    """
    Returns the class for the TaskSpec, if the spec already has the class
    name (str), it just returns the actual class object with such name,
    otherwise it tries to guess based on the source string
    """
    class_name = task_spec.get('class', None)

    if class_name:
        try:
            class_ = validators.string.validate_task_class_name(class_name)
        except Exception as e:
            msg = f'Error validating Task spec (class field): {e.args[0]}'
            e.args = (msg, )
            raise
    else:
        class_ = task_class_from_source_str(
            task_spec['source'],
            lazy_import,
            reload,
            task_spec.get('product'),
        )

    return class_


def _init_source_for_task_class(source_str, task_class, project_root,
                                lazy_import, make_absolute):
    """
    Initialize source. Loads dotted path to callable if a PythonCallable
    task, otherwise it returns a path
    """
    if task_class is tasks.PythonCallable:
        if lazy_import:
            return source_str
        else:
            return dotted_path.load_dotted_path(source_str)
    else:
        path = Path(source_str)

        # NOTE: there is some inconsistent behavior here. project_root
        # will be none if DAGSpec was initialized with a dictionary, hence
        # this won't resolve to absolute paths - this is a bit confusing.
        # maybe always convert to absolute?
        if project_root and not path.is_absolute() and make_absolute:
            return Path(project_root, source_str)
        else:
            return path


class TaskSpec(MutableMapping):
    """
    A TaskSpec converts dictionaries to Task objects. This class is not
    intended to be used directly, but through DAGSpec

    Parameters
    ----------
    data : dict
        The data that holds the spec information
    meta : dict
        The "meta" section information from the calling DAGSpec
    project_root : str or pathlib.Path
        The project root folder. Relative paths in "product" are so to this
        folder
    lazy_import : bool, default=False
        If False, sources are loaded when initializing the spec (e.g.
        a dotted path is imported, a source loaded using a SourceLoader
        is converted to a Placeholder object)
    reload : bool, default=False
        Reloads modules before importing dotted paths to detect code changes
        if the module has already been imported. Has no effect if
        lazy_import=True.
    """
    def __init__(self,
                 data,
                 meta,
                 project_root,
                 lazy_import=False,
                 reload=False):
        self.data = deepcopy(data)
        self.meta = deepcopy(meta)
        self.project_root = project_root
        self.lazy_import = lazy_import

        self.validate()

        source_loader = meta['source_loader']

        # initialize required elements
        self.data['class'] = task_class_from_spec(self.data, lazy_import,
                                                  reload)
        # preprocess source obj, at this point it will either be a Path if the
        # task requires a file or a callable if it's a PythonCallable task
        self.data['source'] = _init_source_for_task_class(
            self.data['source'],
            self.data['class'],
            self.project_root,
            lazy_import,
            # only make sources absolute paths when not using a source loader
            # otherwise keep them relative
            make_absolute=source_loader is None)

        is_path = isinstance(self.data['source'], Path)

        # check if we need to use the source_loader. we don't if the path is
        # relative because that doesn't make sense with a source_loader, and
        # this gives the user the ability to load some files that might
        # not be part of the source loader
        if source_loader and is_path and not self.data['source'].is_absolute():
            if lazy_import:
                self.data['source'] = source_loader.path_to(
                    self.data['source'])
            else:
                self.data['source'] = source_loader[self.data['source']]

    def validate(self):
        """
        Validates the data schema
        """
        if 'upstream' not in self.data:
            self.data['upstream'] = None

        if self.meta['extract_product']:
            required = {'source'}
        else:
            required = {'product', 'source'}

        validate.keys(valid=None,
                      passed=self.data,
                      required=required,
                      name=repr(self))

        if self.meta['extract_upstream'] and self.data.get('upstream'):
            raise ValueError('Error validating task "{}", if '
                             'meta.extract_upstream is set to True, tasks '
                             'should not have an "upstream" key'.format(
                                 self.data))

        if self.meta['extract_product'] and self.data.get('product'):
            raise ValueError('Error validating task "{}", if '
                             'meta.extract_product is set to True, tasks '
                             'should not have a "product" key'.format(
                                 self.data))

    def to_task(self, dag):
        """
        Convert the spec to a Task or TaskGroup and add it to the dag.
        Returns a (task, upstream) tuple with the Task instance and list of
        upstream dependencies (as described in the 'upstream' key, if any,
        empty if no 'upstream' key). If the spec has a 'grid' key, a TaskGroup
        instance instead

        Parameters
        ----------
        dag
            The DAG to add the task(s) to
        """
        data = copy(self.data)
        upstream = _make_iterable(data.pop('upstream'))

        if 'grid' in data:
            if 'params' in data:
                raise KeyError(f'Error initializing task with spec {data!r}: '
                               '\'params\' is not allowed when using \'grid\'')

            if 'name' not in data:
                raise KeyError(f'Error initializing task with spec {data!r}: '
                               'tasks with \'grid\' must have a \'name\'')

            task_class = data.pop('class')
            product_class = _find_product_class(task_class, data, self.meta)
            product = data.pop('product')
            name = data.pop('name')
            grid = data.pop('grid')

            # hooks
            on_render = data.pop('on_render', None)
            on_finish = data.pop('on_finish', None)
            on_failure = data.pop('on_failure', None)

            if on_render:
                on_render = dotted_path.DottedPath(on_render,
                                                   lazy_load=self.lazy_import)

            if on_finish:
                on_finish = dotted_path.DottedPath(on_finish,
                                                   lazy_load=self.lazy_import)

            if on_failure:
                on_failure = dotted_path.DottedPath(on_failure,
                                                    lazy_load=self.lazy_import)

            return TaskGroup.from_grid(task_class=task_class,
                                       product_class=product_class,
                                       product_primitive=product,
                                       task_kwargs=data,
                                       dag=dag,
                                       name=name,
                                       grid=grid,
                                       resolve_relative_to=self.project_root,
                                       on_render=on_render,
                                       on_finish=on_finish,
                                       on_failure=on_failure), upstream
        else:
            return _init_task(data=data,
                              meta=self.meta,
                              project_root=self.project_root,
                              lazy_import=self.lazy_import,
                              dag=dag), upstream

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        for e in self.data:
            yield e

    def __len__(self):
        return len(self.data)

    def __repr__(self):
        return '{}({!r})'.format(type(self).__name__, self.data)


def _init_task(data, meta, project_root, lazy_import, dag):
    """Initialize a single task from a dictionary spec
    """
    task_dict = copy(data)
    class_ = task_dict.pop('class')

    product = _init_product(task_dict,
                            meta,
                            class_,
                            project_root,
                            lazy_import=lazy_import)

    _init_client(task_dict, lazy_import=lazy_import)

    source = task_dict.pop('source')

    name = task_dict.pop('name', None)

    on_finish = task_dict.pop('on_finish', None)
    on_render = task_dict.pop('on_render', None)
    on_failure = task_dict.pop('on_failure', None)

    if 'serializer' in task_dict:
        task_dict['serializer'] = dotted_path.DottedPath(
            task_dict['serializer'], lazy_load=lazy_import)

    if 'unserializer' in task_dict:
        task_dict['unserializer'] = dotted_path.DottedPath(
            task_dict['unserializer'], lazy_load=lazy_import)

    # edge case: if using lazy_import, we should not check if the kernel
    # is installed. this is used when exporting to Argo/Airflow using
    # soopervisor, since the exporting process should not require to have
    # the ir kernel installed. The same applies when Airflow has to convert
    # the DAG, the Airflow environment shouldn't require the ir kernel
    if (class_ == tasks.NotebookRunner and lazy_import
            and 'check_if_kernel_installed' not in task_dict):
        task_dict['check_if_kernel_installed'] = False

    # make paths to resources absolute
    if 'params' in task_dict:
        task_dict['params'] = resolve_resources(task_dict['params'],
                                                relative_to=project_root)

    try:
        task = class_(source=source,
                      product=product,
                      name=name,
                      dag=dag,
                      **task_dict)
    except Exception as e:
        msg = (f'Error initializing {class_.__name__} from {data!r}. '
               f'Error: {e.args[0]}')
        e.args = (msg, )
        raise

    if on_finish:
        task.on_finish = dotted_path.DottedPath(on_finish,
                                                lazy_load=lazy_import)

    if on_render:
        task.on_render = dotted_path.DottedPath(on_render,
                                                lazy_load=lazy_import)

    if on_failure:
        task.on_failure = dotted_path.DottedPath(on_failure,
                                                 lazy_load=lazy_import)

    return task


# FIXME: how do we make a default product client? use the task's client?
def _init_product(task_dict, meta, task_class, root_path, lazy_import):
    """
    Initialize product.

    Resolution logic order:
        task.product_class
        meta.{task_class}.product_default_class

    Current limitation: When there is more than one product, they all must
    be from the same class.
    """
    product_raw = task_dict.pop('product')

    # return if we already have a product
    if isinstance(product_raw, products.product.Product):
        return product_raw

    CLASS = _find_product_class(task_class, task_dict, meta)

    if 'product_client' in task_dict:
        dp = dotted_path.DottedPath(task_dict.pop('product_client'),
                                    lazy_load=lazy_import,
                                    allow_return_none=False)

        if lazy_import:
            client = dp
        else:
            client = dp()

        kwargs = {'client': client}
    else:
        kwargs = {}

    # determine the base path for the product (only relevant if product
    # is a File)
    relative_to = (Path(task_dict['source']).parent
                   if meta['product_relative_to_source'] else root_path)

    # initialize Product instance
    return try_product_init(CLASS, product_raw, relative_to, kwargs)


def _find_product_class(task_class, task_dict, meta):
    key = 'product_default_class.' + task_class.__name__
    meta_product_default_class = get_value_at(meta, key)

    if 'product_class' in task_dict:
        return validate_product_class_name(task_dict.pop('product_class'))
    elif meta_product_default_class:
        return validate_product_class_name(meta_product_default_class)
    else:
        raise ValueError('Could not determine a product class for task: '
                         '"{}". Add an explicit value in the '
                         '"product_class" key or provide a default value in '
                         'meta.product_default_class by setting the '
                         'key to the applicable task class'.format(task_dict))


def try_product_init(class_, product_raw, relative_to, kwargs):
    """Initializes Product (or MetaProduct)

    Parameters
    ----------
    class_ : class
        Product class

    product_raw : str, list or dict
        The raw value as indicated by the user in the pipeline.yaml file. str
        if a single file, list if a SQL relation or dict if a MetaProduct

    relative_to : str
        Prefix for all relative paths (only applicable to File products)

    kwargs : dict
        Other kwargs to initialize product
    """
    if isinstance(product_raw, Mapping):
        return {
            key: _try_product_init(class_,
                                   resolve_if_file(value, relative_to, class_),
                                   kwargs)
            for key, value in product_raw.items()
        }
    else:
        path_to_source = resolve_if_file(product_raw, relative_to, class_)
        return _try_product_init(class_, path_to_source, kwargs)


def _try_product_init(class_, path_to_source, kwargs):
    """
    Try to initialize product, raises a chained exception if not possible.
    To provide more context.
    """
    try:
        return class_(path_to_source, **kwargs)
    except Exception as e:
        kwargs_msg = f' and keyword arguments: {kwargs!r}' if kwargs else ''
        raise DAGSpecInitializationError(
            f'Error initializing {class_.__name__} with source: '
            f'{path_to_source!r}' + kwargs_msg) from e


def validate_product_class_name(product_class_name):
    try:
        return validators.string.validate_product_class_name(
            product_class_name)
    except Exception as e:
        msg = ('Error validating Task spec (product_class field): '
               f'{e.args[0]}')
        e.args = (msg, )
        raise


def resolve_if_file(product_raw, relative_to, class_):
    """Resolve Product argument if it's a File to make it an absolute path
    """
    try:
        return _resolve_if_file(product_raw, relative_to, class_)
    except Exception as e:
        e.args = ('Error initializing File with argument '
                  f'{product_raw!r} ({e})', )
        raise


def _resolve_if_file(product_raw, relative_to, class_):
    """Resolve File argument to make it an absolute path
    """
    # not a file, nothing to do...
    if class_ != products.File:
        return product_raw
    # resolve...
    elif relative_to:
        # To keep things consistent, product relative paths are so to the
        # pipeline.yaml file (not to the current working directory). This is
        # important because there is no guarantee that the process calling
        # this will be at the pipeline.yaml location. One example is
        # when using the integration with Jupyter notebooks, each notebook
        # will set its working directory to the current parent.
        return str(Path(relative_to, product_raw).resolve())
    # no realtive_to argument, nothing to do...
    else:
        return Path(product_raw).resolve()


def _init_client(task_dict, lazy_import):
    if 'client' in task_dict:
        dp = dotted_path.DottedPath(task_dict.pop('client'),
                                    lazy_load=lazy_import,
                                    allow_return_none=False)

        if lazy_import:
            task_dict['client'] = dp
        else:
            task_dict['client'] = dp()


def get_value_at(d, dotted_path):
    current = d

    for key in dotted_path.split('.'):
        try:
            current = current[key]
        except KeyError:
            return None

    return current
