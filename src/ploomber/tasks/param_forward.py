from ploomber.tasks import PythonCallable
from ploomber.products import EmptyProduct

# NOTE: subclassing PythonCallable would allow us to show better error messages
# when calling dag.build() on dags that should be built using InMemoryDAG


def _input_data_passer(product, input_data):
    return input_data


def input_data_passer(dag, name):
    return PythonCallable(_input_data_passer,
                          EmptyProduct(),
                          dag=dag,
                          name=name,
                          params={'input_data': None})


def in_memory_processor(dag, name, processor, **kwargs):
    return PythonCallable(processor,
                          EmptyProduct(),
                          dag=dag,
                          name=name,
                          params=kwargs)
