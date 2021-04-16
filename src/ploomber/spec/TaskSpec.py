"""
Create Tasks from dictionaries
"""
from functools import partial
from copy import copy
from pathlib import Path
from collections.abc import MutableMapping, Mapping

from ploomber import tasks, products
from ploomber.util.util import _make_iterable
from ploomber.util import validate, dotted_path
from ploomber import validators

from ploomber.exceptions import DAGSpecInitializationError

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

    # if lazy load, just locate the module without importing it
    fn_checker = dotted_path.locate_dotted_path if lazy_import else partial(
        dotted_path.load_dotted_path, raise_=True, reload=reload)

    if extension and extension in suffix2taskclass:
        if extension == '.sql' and _safe_suffix(product) in {
                '.csv', '.parquet'
        }:
            return tasks.SQLDump

        return suffix2taskclass[extension]
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
                f'source {source_str!r}. This looks like a dotted path '
                'but it failed to import. Original error '
                f'message: {error!s}. You can also set the task class '
                'using the "class" key')
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


def source_for_task_class(source_str, task_class, project_root, lazy_import,
                          make_absolute):
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
        The project root folder (pipeline.yaml parent)
    lazy_import : bool, default=False
        If False, sources are loaded when initializing the spec (e.g.
        a dotted path is imported, a source loaded using a SourceLoader
        is converted to a Placeholder object)
    reload : bool, default=False
        Reloads modules before getting dotted paths. Has no effect if
        lazy_import=True
    """
    def __init__(self,
                 data,
                 meta,
                 project_root,
                 lazy_import=False,
                 reload=False):
        # FIXME: make sure data and meta are immutable structures
        self.data = data
        self.meta = meta
        self.project_root = project_root
        self.lazy_import = lazy_import

        self.validate()

        source_loader = meta['source_loader']

        # initialize required elements
        self.data['class'] = task_class_from_spec(self.data, lazy_import,
                                                  reload)
        # preprocess source obj, at this point it will either be a Path if the
        # task requires a file or a callable if it's a PythonCallable task
        self.data['source'] = source_for_task_class(
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
        """Converts the spec to a Task instance and adds it to the dag
        """
        task_dict = copy(self.data)
        upstream = _make_iterable(task_dict.pop('upstream'))
        class_ = task_dict.pop('class')

        product = init_product(task_dict, self.meta, class_, self.project_root)

        _init_client(task_dict)

        source = task_dict.pop('source')

        name = task_dict.pop('name', None)

        on_finish = task_dict.pop('on_finish', None)
        on_render = task_dict.pop('on_render', None)
        on_failure = task_dict.pop('on_failure', None)

        if 'serializer' in task_dict:
            task_dict['serializer'] = dotted_path.load_callable_dotted_path(
                task_dict['serializer'])

        if 'unserializer' in task_dict:
            task_dict['unserializer'] = dotted_path.load_callable_dotted_path(
                task_dict['unserializer'])

        # edge case: if using lazy_import, we should not check if the kernel
        # is installed. this is used when exporting to Argo/Airflow using
        # soopervisor, since the exporting process should not require to have
        # the ir kernel installed. The same applies when Airflow has to convert
        # the DAG, the Airflow environment shouldn't require the ir kernel
        if (class_ == tasks.NotebookRunner and self.lazy_import
                and 'check_if_kernel_installed' not in task_dict):
            task_dict['check_if_kernel_installed'] = False

        try:
            task = class_(source=source,
                          product=product,
                          name=name,
                          dag=dag,
                          **task_dict)
        except Exception as e:
            msg = f'Error initializing Task from {self!r}. Error: {e.args[0]}'
            e.args = (msg, )
            raise

        if on_finish:
            task.on_finish = dotted_path.load_callable_dotted_path(on_finish)

        if on_render:
            task.on_render = dotted_path.load_callable_dotted_path(on_render)

        if on_failure:
            task.on_failure = dotted_path.load_callable_dotted_path(on_failure)

        return task, upstream

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


# FIXME: how do we make a default product client? use the task's client?
def init_product(task_dict, meta, task_class, root_path):
    """
    Initialize product.

    Resolution logic order:
        task.product_class
        meta.{task_class}.product_default_class

    Current limitation: When there is more than one product, they all must
    be from the same class.
    """
    product_raw = task_dict.pop('product')

    # if the product is not yet initialized (e.g. scripts extract products
    # as dictionaries, lists or strings)
    if isinstance(product_raw, products.Product):
        return product_raw

    key = 'product_default_class.' + task_class.__name__
    meta_product_default_class = get_value_at(meta, key)

    if 'product_class' in task_dict:
        CLASS = validate_product_class_name(task_dict.pop('product_class'))
    elif meta_product_default_class:
        CLASS = validate_product_class_name(meta_product_default_class)
    else:
        raise ValueError('Could not determine a product class for task: '
                         '"{}". Add an explicit value in the '
                         '"product_class" key or provide a default value in '
                         'meta.product_default_class by setting the '
                         'key to the applicable task class'.format(task_dict))

    if 'product_client' in task_dict:
        kwargs = {
            'client': dotted_path.call_spec(task_dict.pop('product_client'))
        }
    else:
        kwargs = {}

    relative_to = (Path(task_dict['source']).parent
                   if meta['product_relative_to_source'] else root_path)

    if isinstance(product_raw, Mapping):
        return {
            key: try_product_init(CLASS,
                                  resolve_if_file(value, relative_to, CLASS),
                                  kwargs)
            for key, value in product_raw.items()
        }
    else:
        source = resolve_if_file(product_raw, relative_to, CLASS)
        return try_product_init(CLASS, source, kwargs)


def try_product_init(class_, source, kwargs):
    """
    Try to initialize product, raises a chained exception if not possible.
    This provides more contextual information
    """
    try:
        return class_(source, **kwargs)
    except Exception as e:
        kwargs_msg = f'and keyword arguments: {kwargs!r}' if kwargs else ''
        raise DAGSpecInitializationError(
            f'Error initializing {class_.__name__} with source: '
            f'{source!r}' + kwargs_msg) from e


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
    try:
        return _resolve_if_file(product_raw, relative_to, class_)
    except Exception as e:
        e.args = ('Error initializing File with argument '
                  f'{product_raw!r} ({e})', )
        raise


def _resolve_if_file(product_raw, relative_to, class_):
    if class_ != products.File:
        return product_raw
    elif relative_to:
        # To keep things consistent, product relative paths are so to the
        # pipeline.yaml file (not to the current working directory). This is
        # important because there is no guarantee that the process calling
        # this will be at the pipeline.yaml location. One example is
        # when using the integration with Jupyter notebooks, each notebook
        # will set its working directory to the current parent.
        return str(Path(relative_to, product_raw).resolve())
    else:
        return Path(product_raw).resolve()


def _init_client(task_dict):
    if 'client' in task_dict:
        task_dict['client'] = dotted_path.call_spec(task_dict.pop('client'))


def get_value_at(d, dotted_path):
    current = d

    for key in dotted_path.split('.'):
        try:
            current = current[key]
        except KeyError:
            return None

    return current
