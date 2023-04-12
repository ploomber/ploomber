from functools import wraps
from inspect import signature
from itertools import chain

from functools import partial
from ploomber import DAGConfigurator
from ploomber.executors import ParallelDill, Serial
from ploomber.io import serializer
from ploomber.products import File
from ploomber.util.param_grid import ParamGrid
from ploomber.micro._task import _PythonCallableNoValidation
from ploomber.io.serialize import _build_extension_mapping_final
from ploomber.io.unserialize import _EXTERNAL, _DEFAULTS, _unserialize_product


# we should re-use the existing logic in unserialize.py but this will do for
# now
def unserializer(
    extension_mapping=None, *, fallback=False, defaults=None, unpack=False
):
    def _unserializer(fn):
        extension_mapping_final = _build_extension_mapping_final(
            extension_mapping, defaults, fn, _DEFAULTS, "unserializer"
        )

        try:
            unserializer_fallback = _EXTERNAL[fallback]
        except KeyError:
            error = True
        else:
            error = False

        if error:
            raise ValueError(
                f"Invalid fallback argument {fallback!r} "
                f"in function {fn.__name__!r}. Must be one of "
                "True, 'joblib', or 'cloudpickle'"
            )

        if unserializer_fallback is None and fallback in {"cloudpickle", "joblib"}:
            raise ModuleNotFoundError(
                f"Error unserializing with function {fn.__name__!r}. "
                f"{fallback} is not installed"
            )

        n_params = len(signature(fn).parameters)
        if n_params != 1:
            raise TypeError(
                f"Expected unserializer {fn.__name__!r} "
                f"to take 1 argument, but it takes {n_params!r}"
            )

        @wraps(fn)
        def wrapper(product):
            return _unserialize_product(
                product,
                extension_mapping_final,
                fallback,
                unserializer_fallback,
                fn,
                unpack,
            )

        return wrapper

    return _unserializer


@serializer(fallback=True)
def _serializer(obj, product):
    pass


@unserializer(fallback=True)
def _unserializer(product):
    pass


# TODO: order should not (?) matter, add tests that have @capture
# and @grid in both orders. it gets a little complidated since @grid can
# appear many times, so maybe we should enforce order?


def grid(**params):
    """A decorator to create multiple tasks, one per parameter combination"""

    def decorator(f):
        if not hasattr(f, "__ploomber_grid__"):
            f.__ploomber_grid__ = []

        # TODO: validate they have the same keys as the earlier ones
        f.__ploomber_grid__.append(params)
        return f

    return decorator


def capture(f):
    """A decorator to capture outputs in a function"""
    f.__ploomber_capture__ = True
    f.__ploomber_globals__ = f.__globals__
    return f


def _signature_wrapper(f, call_with_args):
    """An internal wrapper so functions don't need the upstream and product
    arguments
    """
    # store the wrapper, we'll need this for hot_reload to work, see
    # the constructor of CallableLoader in
    # ploomber.sources.pythoncallablesource for details
    f.__ploomber_wrapper_factory__ = partial(
        _signature_wrapper, call_with_args=call_with_args
    )

    @wraps(f)
    def wrapper_args(upstream, **kwargs):
        return f(*upstream.values(), **kwargs)

    @wraps(f)
    def wrapper_kwargs(upstream, **kwargs):
        return f(**upstream, **kwargs)

    return wrapper_args if call_with_args else wrapper_kwargs


def _get_upstream(fn):
    """
    Get upstream tasks for a given function by looking at the signature
    arguments
    """
    if hasattr(fn, "__wrapped__"):
        grid = getattr(fn, "__ploomber_grid__", None)

        if grid is not None:
            ignore = set(grid[0])
        else:
            ignore = set()

        return set(signature(fn.__wrapped__).parameters) - ignore
    else:
        return set(signature(fn).parameters) - {"input_data"}


def _make_task(callable_, dag, params, output, call_with_args, suffix=None):
    """Generate a Ploomber task from a function"""
    name = callable_.__name__
    name = name if suffix is None else f"{name}-{suffix}"
    params_signature = set(signature(callable_).parameters)

    if len(params_signature) and params_signature != {"input_data"}:
        # wrap the callable_ so it looks like a function with an "upstream"
        callable_ = _signature_wrapper(callable_, call_with_args=call_with_args)

    CLASS = _PythonCallableNoValidation

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


def dag_from_functions(
    functions,
    output="output",
    params=None,
    parallel=False,
    dependencies=None,
    hot_reload=True,
):
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

    hot_reload : bool, default=False
        If True, the dag will automatically detect changes in the source code.
        However, this won't work if tasks are defined inside functions
    """
    dependencies = dependencies or dict()
    params = params or dict()

    # NOTE: cache_rendered_status isn't doing anything. I modified Product
    # to look at the hot_reload flag for the hot reloading to work. I don't
    # think we're using the cache_rendered_status anywhere, so we should
    # delete it.
    configurator = DAGConfigurator()
    configurator.params.hot_reload = hot_reload
    dag = configurator.create()

    if parallel:
        dag.executor = ParallelDill()
    else:
        # need to disable subprocess, otherwise pickling will fail since
        # functions might be defined in the __main__ module
        dag.executor = Serial(build_in_subprocess=False)

    for callable_ in functions:
        if callable_.__name__ in params:
            params_task = params[callable_.__name__]
        else:
            params_task = dict()

        # if decorated, call with grid
        if hasattr(callable_, "__ploomber_grid__"):
            for i, items in enumerate(
                chain(
                    *(ParamGrid(grid).product() for grid in callable_.__ploomber_grid__)
                )
            ):
                _make_task(
                    callable_,
                    dag=dag,
                    params={**params_task, **items},
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
