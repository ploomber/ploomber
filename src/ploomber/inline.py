"""
This module contains utilities for defining inline pipelines. That is, defining
and running pipelines inside a Jupyter notebook
"""
from functools import wraps
from inspect import signature

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.executors import ParallelDill, Serial
from ploomber.sources import PythonCallableSource
from ploomber.io import serializer, unserializer


@serializer(fallback=True)
def _serializer(obj, product):
    pass


@unserializer(fallback=True)
def _unserializer(product):
    pass


def signature_wrapper(f):

    @wraps(f)
    def wrapper(upstream):
        return f(**upstream)

    return wrapper


def _get_upstream(fn):
    if hasattr(fn, '__wrapped__'):
        return list(signature(fn.__wrapped__).parameters)
    else:
        return []


class _NoValidationSource(PythonCallableSource):

    def _post_render_validation(self, rendered_value, params):
        pass


class _PythonCallableNoValidation(PythonCallable):

    @staticmethod
    def _init_source(source, kwargs):
        return _NoValidationSource(source, **kwargs)


def _make_task(callable_, dag, params, output):
    name = callable_.__name__

    if set(signature(callable_).parameters) != {'input_data'}:
        callable_ = signature_wrapper(callable_)

    task = _PythonCallableNoValidation(callable_,
                                       File(f'{output}/{name}'),
                                       dag=dag,
                                       name=name,
                                       serializer=_serializer,
                                       unserializer=_unserializer,
                                       params=params)
    return task


def dag_from_functions(functions,
                       output='output',
                       params=None,
                       parallel=False):
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

    parallel : bool default=False
        If True, the dag will run tasks in parallel when calling
        ``dag.build()``, note that this requires the 'multiprocess' package:
        ``pip install multiprocess``
    """

    params = params or dict()

    if parallel:
        dag = dag = DAG(executor=ParallelDill())
    else:
        dag = DAG(executor=Serial(build_in_subprocess=False))

    for callable_ in functions:
        if callable_.__name__ in params:
            params_task = params[callable_.__name__]
        else:
            params_task = None

        _make_task(callable_, dag=dag, params=params_task, output=output)

    for task in dag._iter():
        upstream = _get_upstream(dag[task].source.primitive)
        for up in upstream:
            dag[task].set_upstream(dag[up])

    return dag
