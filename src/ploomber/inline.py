"""
This module contains utilities for defining inline pipelines. That is, defining
and running pipelines inside a Jupyter notebook
"""
import re
import contextlib
import inspect
import os
import sys
from functools import wraps, partial
from inspect import signature
from itertools import chain
from pathlib import Path

import debuglater
import nbconvert
import nbformat
import parso
from ploomber_engine.ipython import PloomberClient

from ploomber import DAG
from ploomber.exceptions import TaskBuildError
from ploomber.executors import ParallelDill, Serial
from ploomber.io import serializer, unserializer
from ploomber.products import File
from ploomber.sources import PythonCallableSource
from ploomber.tasks import PythonCallable
from ploomber.tasks.tasks import _unserialize_params
from ploomber.util.debug import debug_if_exception
from ploomber.util.param_grid import ParamGrid


@serializer(fallback=True)
def _serializer(obj, product):
    pass


@unserializer(fallback=True)
def _unserializer(product):
    pass


def grid(**params):

    def decorator(f):
        if not hasattr(f, "__ploomber_grid__"):
            f.__ploomber_grid__ = []

        # TODO: validate they have the same keys as the earlier ones
        f.__ploomber_grid__.append(params)
        return f

    return decorator


def capture(f):
    f.__ploomber_capture__ = True
    f.__ploomber_globals__ = f.__globals__
    return f


def signature_wrapper(f, call_with_args):

    @wraps(f)
    def wrapper_args(upstream, **kwargs):
        return f(*upstream.values(), **kwargs)

    @wraps(f)
    def wrapper_kwargs(upstream, **kwargs):
        return f(**upstream, **kwargs)

    return wrapper_args if call_with_args else wrapper_kwargs


def _get_upstream(fn):
    if hasattr(fn, "__wrapped__"):
        grid = getattr(fn, "__ploomber_grid__", None)

        if grid is not None:
            ignore = set(grid[0])
        else:
            ignore = set()

        return set(signature(fn.__wrapped__).parameters) - ignore
    else:
        return set(signature(fn).parameters) - {'input_data'}


class _NoValidationSource(PythonCallableSource):

    def _post_render_validation(self, rendered_value, params):
        pass


class _PythonCallableNoValidation(PythonCallable):

    @staticmethod
    def _init_source(source, kwargs):
        return _NoValidationSource(source, **kwargs)


def _make_task(callable_, dag, params, output, call_with_args, suffix=None):
    name = callable_.__name__
    name = name if suffix is None else f"{name}-{suffix}"
    params_signature = set(signature(callable_).parameters)

    if len(params_signature) and params_signature != {"input_data"}:
        # wrap the callable_ so it looks like a function with an "upstream"
        callable_ = signature_wrapper(callable_, call_with_args=call_with_args)

    capture_ = hasattr(callable_, '__ploomber_capture__')
    CLASS = (_PythonCallableNoValidation
             if not capture_ else _CapturedPythonCallable)

    if capture_:
        product = {
            'return': File(f"{output}/{name}"),
            'html': File(f"{output}/{name}.html"),
            'ipynb': File(f"{output}/{name}.ipynb")
        }
    else:
        product = File(f"{output}/{name}")

    task = CLASS(
        callable_,
        product,
        dag=dag,
        name=name,
        serializer=_serializer,
        unserializer=_unserializer,
        params=params,
    )
    return task


def dag_from_functions(functions,
                       output="output",
                       params=None,
                       parallel=False,
                       dependencies=None):
    """Create a DAG from a list of functions

    Parameters
    ----------
    functions : list
        List of functions

    output : str, default='output'
        Directory to store outputs and metadata from each task

    params : dict, default None
        Parameters to pass to each task, it must be a dictionary with task
        names as keys and parameters (dict) as values

    parallel : bool, default=False
        If True, the dag will run tasks in parallel when calling
        ``dag.build()``, note that this requires the 'multiprocess' package:
        ``pip install multiprocess``

    dependencies : dict, default=None
        A mapping with functions names to their dependencies. Use it if
        the arguments in the function do not match the names of its
        dependencies.
    """
    dependencies = dependencies or dict()
    params = params or dict()

    if parallel:
        dag = DAG(executor=ParallelDill())
    else:
        # need to disable subprocess, otherwise pickling will fail since
        # functions might be defined in the __main__ module
        dag = DAG(executor=Serial(build_in_subprocess=False))

    for callable_ in functions:
        if callable_.__name__ in params:
            params_task = params[callable_.__name__]
        else:
            params_task = dict()

        # if decorated, call with grid
        if hasattr(callable_, "__ploomber_grid__"):

            for i, items in enumerate(
                    chain(*(ParamGrid(grid).product()
                            for grid in callable_.__ploomber_grid__))):

                _make_task(
                    callable_,
                    dag=dag,
                    params={
                        **params_task,
                        **items
                    },
                    output=output,
                    call_with_args=callable_.__name__ in dependencies,
                    suffix=i,
                )
        else:

            _make_task(
                callable_,
                dag=dag,
                params=params_task,
                output=output,
                call_with_args=callable_.__name__ in dependencies,
            )

    for name in dag._iter():
        # check if there are manually declared dependencies
        if name in dependencies:
            upstream = dependencies[name]
        else:
            upstream = _get_upstream(dag[name].source.primitive)

        for up in upstream:
            dag[name].set_upstream(dag[up])

    return dag


# EXPERIMENTAL


class _CapturedPythonCallable(_PythonCallableNoValidation):

    def run(self):
        if "upstream" in self.params and self._unserializer:
            # self.params["upstream"]["ones"] =
            # self.params["upstream"]["ones"][
            #     "return"]
            params = _unserialize_params(self.params, self._unserializer)
        else:
            params = self.params.to_dict()

        # do not pass product if serializer is set, we'll use the returned
        # value in such case
        if self._serializer:
            product = params.pop("product")
        else:
            product = params["product"]

        callable_ = partial(
            capture_execute,
            function=self.source.primitive,
            globals_=self.source.primitive.__ploomber_globals__,
            **params,
        )

        if self.debug_mode == "later":
            try:
                nb, out = callable_()
            except Exception as e:
                debuglater.run(self.name, echo=False)
                path_to_dump = f"{self.name}.dump"
                message = (f"Serializing traceback to: {path_to_dump}. "
                           f"To debug: dltr {path_to_dump}")
                raise TaskBuildError(message) from e
        elif self.debug_mode == "now":
            nb, out = debug_if_exception(callable_=callable_,
                                         task_name=self.name)
        else:
            nb, out = callable_()
            Path(self.product['ipynb']).write_text(nbformat.v4.writes(nb))
            to_html(self.product["html"], nb)

        # serialize output if needed
        if self._serializer:
            if out is None:
                raise ValueError("Callable {} must return a value if task "
                                 "is initialized with a serializer".format(
                                     self.source.primitive))
            else:
                self._serializer(out, product["return"])


@contextlib.contextmanager
def add_to_sys_path(path, chdir=False):
    """
    Add directory to sys.path, optionally making it the working directory
    temporarily
    """
    cwd_old = os.getcwd()

    if path is not None:
        path = os.path.abspath(path)
        sys.path.insert(0, path)

        if chdir:
            os.chdir(path)

    try:
        yield
    finally:
        if path is not None:
            sys.path.remove(path)
            os.chdir(cwd_old)


class Client(PloomberClient):

    def execute(self):
        execution_count = 1
        returned_value = None

        with add_to_sys_path("."):
            for index, cell in enumerate(self._nb.cells):
                if cell.cell_type == "code":

                    try:
                        self.execute_cell(
                            cell,
                            cell_index=index,
                            execution_count=execution_count,
                            store_history=False,
                        )
                    except SyntaxError as e:
                        err = e
                    else:
                        err = None
                        tag = parse_tag(cell.source)

                        if tag:
                            cell.metadata = dict(tags=[tag])

                    if err and str(err.msg) == "'return' outside function":
                        r = parso.parse(cell.source.strip()).children[0]
                        var = r.children[-1]
                        returned_value = self._shell.user_ns[var.value]
                        # do not show error traceback
                        cell.outputs = []

                    execution_count += 1

        return self._nb, returned_value


def capture_execute(function, globals_, **kwargs):
    nb = nbformat.v4.new_notebook()

    for statement in get_body_statements(function):
        c = nbformat.v4.new_code_cell(source=statement)
        nb.cells.append(c)

    client = Client(nb)
    upstream = kwargs.pop('upstream', {})
    client._shell.user_ns = {
        **client._shell.user_ns,
        **globals_,
        **upstream,
        **kwargs
    }
    nb, val = client.execute()

    return nb, val


def get_body_statements(function):
    source = inspect.getsource(function)

    # get first non empty child, the function is usually the first one. one
    # case where it's not is when the function is indented (e.g. a function
    # inside a function)
    for child in parso.parse(source).children:
        if child.get_code().strip():
            fn = child
            break

    # NOTE: I think we can remove the while loop
    while fn.children[0].type in {'decorator', 'decorators'}:
        fn = fn.children[1]

    return [
        st.get_code() for st in fn.children[-1].children
        if st.get_code().strip()
    ]


def to_html(path, nb):
    Path(path).write_text(
        nbconvert.export(nbconvert.exporters.HTMLExporter,
                         nb,
                         exclude_input=True)[0])


def parse_tag(source):
    tag_maybe = source.strip().splitlines()[0]
    result = re.match(r'# tag=([\w-]+)', tag_maybe)

    if result:
        return result.group(1)
