"""
Task implementations

A Task is a unit of work that produces a persistent change (Product)
such as a bash or a SQL script"""

import pdb
import functools
from collections.abc import Mapping

import debuglater
from IPython.terminal.debugger import TerminalPdb, Pdb

from ploomber.tasks.abc import Task
from ploomber.tasks.mixins import ClientMixin
from ploomber.sources import PythonCallableSource, GenericSource, EmptySource
from ploomber.clients import ShellClient
from ploomber.products.metadata import MetadataAlwaysUpToDate
from ploomber.exceptions import TaskBuildError, MissingClientError
from ploomber.constants import TaskStatus
from ploomber.sources.interact import CallableInteractiveDeveloper
from ploomber.tasks._params import Params
from ploomber.io.loaders import _file_load
from ploomber.io import _validate
from ploomber.products import MetaProduct
from ploomber.util.debug import debug_if_exception


def _unserializer(product, unserializer):
    # this happens when we have a task group. e.g., product declares
    # 'fit-*' as dependency, which matches fit-1, fit-2, etc. Then product
    # becomes...
    # {'fit-*': {'fit-1': product, 'fit-2': another, ...}}
    if isinstance(product, Mapping) and not isinstance(product, MetaProduct):
        return {k: unserializer(p) for k, p in product.items()}
    else:
        # pass metaproduct and products directly to the unserializer function
        return unserializer(product)


def _unserialize_params(params_original, unserializer):
    """
    User the user-provided function to unserialize params['upstream']
    """
    params = params_original.to_dict()

    params["upstream"] = {
        k: _unserializer(v, unserializer) for k, v in params["upstream"].items()
    }

    params = Params._from_dict(params, copy=False)

    return params


class PythonCallable(Task):
    """
    Execute a Python function

    Parameters
    ----------
    source: callable
        The callable to execute
    product: ploomber.products.Product
        Product generated upon successful execution
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    params: dict
        Parameters to pass to the callable, by default, the callable will
        be executed with a "product" (which will contain the product object).
        It will also include a "upstream" parameter if the task has upstream
        dependencies along with any parameters declared here
    unserializer: callable, optional
        A callable to unserialize upstream products, the product object
        is passed as unique argument. If None, the source function receives
        the product object directly. If the task has no upstream dependencies,
        this argument has no effect
    serializer: callable, optional
        A callable to serialize this task's product, must take two arguments,
        the first argument passed is the value returned by the task's
        source, the second argument is the product oject. If None, the
        task's source is responsible for serializing its own product. If
        used, the source function must not have a "product" parameter but
        return its result instead
    debug_mode : None, 'now'  or 'later', default=None
        If 'now', runs notebook in debug mode, this will start debugger if an
        error is thrown. If 'later', it will serialize the traceback for later
        debugging. (Added in 0.20)

    Examples
    --------

    Spec API:

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
          - source: my_functions.my_task
            product: data.csv


    .. code-block:: python
        :class: text-editor

        # content of my_functions.py
        from pathlib import Path

        def my_task(product):
            Path(product).touch()


    Spec API (multiple outputs):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
          - source: my_functions.another_task
            product:
                one: one.csv
                another: another.csv


    .. code-block:: python
        :class: text-editor

        # content of my_functions.py
        from pathlib import Path

        def another_task(product):
            Path(product['one']).touch()
            Path(product['another']).touch()

    Python API:

    >>> from pathlib import Path
    >>> from ploomber import DAG
    >>> from ploomber.tasks import PythonCallable
    >>> from ploomber.products import File
    >>> from ploomber.executors import Serial
    >>> dag = DAG(executor=Serial(build_in_subprocess=False))
    >>> def my_function(product):
    ...     # create data.csv
    ...     Path(product).touch()
    >>> PythonCallable(my_function, File('data.csv'), dag=dag)
    PythonCallable: my_function -> File('data.csv')
    >>> summary = dag.build()

    Python API (multiple products):


    >>> from pathlib import Path
    >>> from ploomber import DAG
    >>> from ploomber.tasks import PythonCallable
    >>> from ploomber.products import File
    >>> from ploomber.executors import Serial
    >>> dag = DAG(executor=Serial(build_in_subprocess=False))
    >>> def my_function(product):
    ...     Path(product['first']).touch()
    ...     Path(product['second']).touch()
    >>> product = {'first': File('first.csv'),
    ...            'second': File('second.csv')}
    >>> task = PythonCallable(my_function, product, dag=dag)
    >>> summary = dag.build()


    Notes
    -----
    .. collapse:: changelog

        .. versionadded:: 0.20
            ``debug`` constructor flag renamed to ``debug_mode`` to avoid
            conflicts with the ``debug`` method.

    More `examples using the Python API. <https://github.com/ploomber/projects/tree/master/python-api-examples>`_

    The ``executor=Serial(build_in_subprocess=False)`` argument is only
    required if copy-pasting the example in a Python session. If you store the
    code in a script, you may delete it and call ``dag.build`` like this:

    .. code-block:: py
        :class: text-editor

        if __name__ == '__main__':
            dag.build()

    Then call your script:

    .. code-block:: console
        :class: text-editor

        python script.py
    """  # noqa

    def __init__(
        self,
        source,
        product,
        dag,
        name=None,
        params=None,
        unserializer=None,
        serializer=None,
        debug_mode=None,
    ):
        self._serializer = serializer or dag.serializer
        kwargs = dict(
            hot_reload=dag._params.hot_reload, needs_product=self._serializer is None
        )
        self._source = type(self)._init_source(source, kwargs)
        self._unserializer = unserializer or dag.unserializer
        self.debug_mode = debug_mode
        super().__init__(product, dag, name, params)

    @property
    def debug_mode(self):
        return self._debug_mode

    @debug_mode.setter
    def debug_mode(self, value):
        _validate.is_in(value, {None, "now", "later"}, "debug_mode")
        self._debug_mode = value

    @staticmethod
    def _init_source(source, kwargs):
        return PythonCallableSource(source, **kwargs)

    def run(self):
        if "upstream" in self.params and self._unserializer:
            params = _unserialize_params(self.params, self._unserializer)
        else:
            params = self.params.to_dict()

        # do not pass product if serializer is set, we'll use the returned
        # value in such case
        if self._serializer:
            product = params.pop("product")
        else:
            product = params["product"]

        if self.debug_mode == "later":
            try:
                out = self.source.primitive(**params)
            except Exception as e:
                debuglater.run(self.name, echo=False)
                path_to_dump = f"{self.name}.dump"
                message = (
                    f"Serializing traceback to: {path_to_dump}. "
                    f"To debug: dltr {path_to_dump}"
                )
                raise TaskBuildError(message) from e
        elif self.debug_mode == "now":
            out = debug_if_exception(self.source.primitive, self.name, params)
        else:
            out = self.source.primitive(**params)

        # serialize output if needed
        if self._serializer:
            if out is None:
                raise ValueError(
                    "Callable {} must return a value if task "
                    "is initialized with a serializer".format(self.source.primitive)
                )
            else:
                self._serializer(out, product)

    def _interactive_developer(self):
        """
        Creates a CallableInteractiveDeveloper instance
        """
        # TODO: resolve to absolute to make relative paths work
        return CallableInteractiveDeveloper(self.source.primitive, self.params)

    def debug(self, kind="ipdb"):
        """
        Run callable in debug mode.

        Parameters
        ----------
        kind : str ('ipdb' or 'pdb')
            Which debugger to use 'ipdb' for IPython debugger or 'pdb' for
            debugger from the standard library

        Notes
        -----
        Be careful when debugging tasks. If the task has run
        successfully, you overwrite products but don't save the
        updated source code, your DAG will enter an inconsistent state where
        the metadata won't match the overwritten product.
        """
        opts = {"ipdb", "pdb"}

        if kind == "pm":
            raise ValueError(
                "Post-mortem debugging is not supported " "via the .debug() method."
            )

        if kind not in opts:
            raise ValueError('"kind" must be one of {}, got: "{}"'.format(opts, kind))

        if self.exec_status == TaskStatus.WaitingRender:
            raise TaskBuildError(
                'Error in task "{}". '
                "Cannot call task.debug() on a task that has "
                "not been "
                "rendered, call DAG.render() first".format(self.name)
            )

        if "upstream" in self.params and self._unserializer:
            params = _unserialize_params(self.params, self._unserializer)
        else:
            params = self.params.to_dict()

        if self._serializer:
            params.pop("product")

        if kind == "ipdb":
            try:
                # this seems to only work in a Terminal
                ipdb = TerminalPdb()
            except Exception:
                # this works in a Jupyter notebook
                ipdb = Pdb()

            ipdb.runcall(self.source.primitive, **params)
        elif kind == "pdb":
            pdb.runcall(self.source.primitive, **params)

    def load(self, key=None, **kwargs):
        """
        Loads the product. It uses the unserializer function if any, otherwise
        it tries to load it based on the file extension

        Parameters
        ----------
        key
            Key to load, if this task generates more than one product

        **kwargs
            Arguments passed to the unserializer function
        """
        if isinstance(self.product, MetaProduct) and key is None:
            raise ValueError(
                f"Task {self!r} generates multiple products, "
                'use the "key" argument to load one'
            )

        prod = self.product if not key else self.product[key]

        if self._unserializer is not None:
            return self._unserializer(str(prod), **kwargs)
        else:
            return _file_load(prod, **kwargs)


# FIXME: there is already a TaskFactory, this is confusing
def task_factory(_func=None, **factory_kwargs):
    """Syntactic sugar for building PythonCallable tasks"""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(**wrapper_kwargs):
            kwargs = {**factory_kwargs, **wrapper_kwargs}
            return PythonCallable(func, **kwargs)

        return wrapper

    return decorator if _func is None else decorator(_func)


class ShellScript(ClientMixin, Task):
    """Execute a shell script.

    Parameters
    ----------
    source: str or pathlib.Path
        Script source, if str, the content is interpreted as the actual
        script, if pathlib.Path, the content of the file is loaded. The
        souce code must have the {{product}} tag
    product: ploomber.products.Product
        Product generated upon successful execution
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    client: ploomber.clients.ShellClient or RemoteShellClient, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]
    params: dict, optional
        Parameters to pass to the script, by default, the callable will
        be executed with a "product" (which will contain the product object).
        It will also include a "upstream" parameter if the task has upstream
        dependencies along with any parameters declared here. The source
        code is converted to a jinja2.Template for passing parameters,
        refer to jinja2 documentation for details

    Examples
    --------
    Spec API:

    :doc:`See here. </user-guide/shell>`

    Python API:

    >>> from pathlib import Path
    >>> from ploomber import DAG
    >>> from ploomber.tasks import ShellScript
    >>> from ploomber.products import File
    >>> code = "touch {{product['first']}}; touch {{product['second']}}"
    >>> _ = Path('script.sh').write_text(code)
    >>> dag = DAG()
    >>> product = {'first': File('first.txt'), 'second': File('second.txt')}
    >>> _ = ShellScript(Path('script.sh'), product, dag=dag)
    >>> summary = dag.build()

    """

    def __init__(self, source, product, dag, name=None, client=None, params=None):
        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(source, kwargs)
        super().__init__(product, dag, name, params)
        self._client = client

    @property
    def client(self):
        try:
            client = super().client
        except MissingClientError:
            self._client = ShellClient()
            return self._client
        else:
            return client

    @staticmethod
    def _init_source(source, kwargs):
        required = {
            "product": ("ShellScript must include {{product}} in " "its source")
        }

        return GenericSource(source, **kwargs, required=required)

    def run(self):
        self.client.execute(str(self.source))


class DownloadFromURL(Task):
    """
    Download a file from a URL (uses urllib.request.urlretrieve)

    Parameters
    ----------
    source: str
        URL to download the file from
    product: ploomber.products.File
        Product generated upon successful execution
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    """

    def __init__(self, source, product, dag, name=None, params=None):
        params = params or {}
        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(source, kwargs)
        super().__init__(product, dag, name, params)

    def run(self):
        # lazily load urllib as it is slow to import
        from urllib import request

        request.urlretrieve(str(self.source), filename=str(self.product))

    @staticmethod
    def _init_source(source, kwargs):
        return GenericSource(str(source), **kwargs, optional=["product"])


class Link(Task):
    """
    A dummy Task used to "plug" an external Product to a pipeline, this
    task is always considered up-to-date

    The purpose of this Task is to link a pipeline to an external read-only
    file, this task does not do anything on the dataset and the product is
    always considered up-to-date. There are two primary use cases:
    when the raw data is automatically uploaded to a file (or table) and the
    pipeline does not have control over data updates, this task can be used
    to link the pipeline to that file, without having to copy it, downstream
    tasks will see this dataset as just another Product. The second use case
    is when developing a prediction pipeline. When making predictions on new
    data, the pipeline might rely on existing data to generate features,
    this task can be used to point to such file it can also be used to
    point to a serialized model, this last scenario is only recommended
    for prediction pipeline that do not have strict performance requirements,
    unserializing models is an expensive operation, for real-time predictions,
    the model should be kept in memory

    Parameters
    ----------
    product: ploomber.products.Product
        Product to link to the dag
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    """

    def __init__(self, product, dag, name):
        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(kwargs)
        super().__init__(product, dag, name, None)

        # patch product's metadata
        self.product.metadata = MetadataAlwaysUpToDate()
        # product's code will never be considered outdated
        self.product._outdated_code_dependency = self._false

        if not self.product.exists():
            raise RuntimeError(
                "Link tasks should point to Products that "
                'already exist. "{}" task product "{}" does '
                "not exist".format(self.name, self.product)
            )

    def run(self):
        pass

    def set_upstream(self, other):
        raise RuntimeError("Link tasks should not have upstream dependencies")

    @staticmethod
    def _init_source(kwargs):
        return EmptySource(None, **kwargs)

    def _false(self):
        # this should be __false but we can't due to
        # https://bugs.python.org/issue33007
        return False


class Input(Task):
    """
    A dummy task used to represent input provided by the user, it is always
    considered outdated.

    When making new predictions, the user must submit some input data to build
    features and then feed the model, this task can be used to point to
    such input. It does not perform any processing (read-only data) but it is
    always considered outdated, which means it will always trigger execution.

    Parameters
    ----------
    product: ploomber.products.Product
        Product to to serve as input to the dag
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    """

    def __init__(self, product, dag, name):
        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(kwargs)
        super().__init__(product, dag, name, None)

        # do not save metadata (Product's location is read-only)
        self.product.metadata.update = self._null_update_metadata

        # the product will always be considered outdated
        self.product._outdated_data_dependencies = self._true
        self.product._outdated_code_dependency = self._true

        if not self.product.exists():
            raise RuntimeError(
                "Input tasks should point to Products that "
                'already exist. "{}" task product "{}" does '
                "not exist".format(self.name, self.product)
            )

    def run(self):
        pass

    def set_upstream(self, other):
        raise RuntimeError("Input tasks should not have upstream " "dependencies")

    @staticmethod
    def _init_source(kwargs):
        return EmptySource(None, **kwargs)

    def _null_update_metadata(self, source_code, params):
        # this should be __null_update_metadata but we can't due to
        # https://bugs.python.org/issue33007
        pass

    def _true(self):
        # this should be __true but we can't due to
        # https://bugs.python.org/issue33007
        return True
