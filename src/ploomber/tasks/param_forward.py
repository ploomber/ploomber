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
    """
    return PythonCallable(_input_data_passer,
                          EmptyProduct(),
                          dag=dag,
                          name=name,
                          params={
                              'input_data': None,
                              'preprocessor': preprocessor
                          })


def in_memory_callable(callable_, dag, name, params):
    """
    Returns a special in-memory task that runs a callable with the given
    params. When calling InMemoryDAG.build(params), this task should bot appear
    in the input_data dictionary
    """
    return PythonCallable(callable_,
                          EmptyProduct(),
                          dag=dag,
                          name=name,
                          params=params)
