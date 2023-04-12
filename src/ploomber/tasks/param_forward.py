from ploomber.tasks import PythonCallable
from ploomber.products import EmptyProduct

# NOTE: subclassing PythonCallable would allow us to show better error messages
# when calling dag.build() on dags that should be built using InMemoryDAG


def _input_data_passer(input_data, preprocessor):
    if preprocessor is None:
        return input_data
    else:
        return preprocessor(input_data)


def input_data_passer(dag, name, preprocessor=None):
    """
    Returns a special in-memory task that forwards input data as product to
    downstream tasks.

    Parameters
    ----------
    dag : ploomber.DAG
        DAG where the task should be added

    name : str
        Task name

    preprocessor : callable, default=None
        An arbitrary callable that can be used to add custom logic to the
        input data before passing it to downstream tasks


    Returns
    -------
    PythonCallable
        A PythonCallable task with special characteristics. It cannot be
        invoked directly, but through ``InMemoryDAG(dag).build()``
    """
    return PythonCallable(
        _input_data_passer,
        EmptyProduct(),
        dag=dag,
        name=name,
        params={"input_data": None, "preprocessor": preprocessor},
    )


def in_memory_callable(callable_, dag, name, params):
    """
    Returns a special in-memory task that runs a callable with the given
    params. When calling ``InMemoryDAG(dag).build()``, this task should not
    appear in the input_data dictionary

    Parameters
    ----------
    callable_ : callable
        An arbitrary callable to execute

    dag : ploomber.DAG
        DAG where the task should be added

    name : str
        Task name

    params : dict
        Parameters to pass when calling ``callable_``. e.g passing
        ``params=dict(a=1)``, is equivalent to calling ``callable_`` with
        ``a=1``.

    Returns
    -------
    PythonCallable
        A PythonCallable task with special characteristics. It cannot be
        invoked directly, but through ``InMemoryDAG(dag).build()``
    """
    return PythonCallable(callable_, EmptyProduct(), dag=dag, name=name, params=params)
