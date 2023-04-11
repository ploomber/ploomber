import copy as copy_module

from ploomber.util import validate
from ploomber.tasks import PythonCallable


def _do_nothing(x):
    return x


def _non_callables(dag):
    non_callables = []

    for name in dag._iter():
        task = dag[name]

        if not isinstance(task, PythonCallable):
            non_callables.append(task)

    return non_callables


class InMemoryDAG:
    """
    Converts a DAG to a DAG-like object that performs all operations in memory
    (products are not serialized). For this to work all tasks must be
    ``PythonCallable`` objects initialized with callables that return a value
    with valid ``serializer`` and ``unserializer`` parameters.

    Parameters
    ----------
    dag : ploomber.DAG
        The DAG to use

    Examples
    --------
    .. literalinclude:: ../../../examples/InMemoryDAG.py
    """

    def __init__(self, dag, return_postprocessor=None):
        non_callables = _non_callables(dag)

        if non_callables:
            non_callables_repr = ", ".join(repr(t) for t in non_callables)
            raise TypeError(
                "All tasks in the DAG must be PythonCallable, "
                "got unallowed "
                f"types: {non_callables_repr}"
            )

        # if the tasks are not initialized with a serializer in the partial
        # (this happens when the partial is imported in a pipeline.yaml file
        # and the latter defines DAG-level serializers), the source will
        # be initialized with .source._needs_product=True. We won't pass
        # product here so we force it to False. Othersie dag.render() will fail
        for name in dag._iter():
            dag[name].source._needs_product = False

        dag.render()

        self.dag = dag
        self.root_nodes = [name for name, degree in dag._G.in_degree() if not degree]
        self.return_postprocessor = return_postprocessor or _do_nothing

        # TODO: validate that root nodes have single parameter in the signature
        # input_data. and was initialized with {'input_data': None}

        # TODO: maybe raise a warning if the DAG has attributes that won't be
        # used (like serializer, unserializer). Or just document it?

    def build(self, input_data, copy=False):
        """Run the DAG

        Parameters
        ----------
        input_data : dict
            A dictionary mapping root tasks (names) to dict params. Root tasks
            are tasks in the DAG that do not have upstream dependencies,
            the corresponding dictionary is passed to the respective task
            source function as keyword arguments

        copy : bool or callable
            Whether to copy the output of an upstream task before passing it
            to the task being processed. It is recommended to turn this off
            for memory efficiency but if the tasks are not pure functions
            (i.e. mutate their inputs) this migh lead to bugs, in such
            case, the best way to fix it would be to make all your tasks
            pure functions but you can enable this option if memory
            consumption is not a problem. If True it uses the ``copy.copy``
            function before passing the upstream products, if you pass a
            callable instead, such function is used (for example, you
            may pass ``copy.deepcopy``)

        Returns
        -------
        dict
            A dictionary mapping task names to their respective outputs
        """
        outs = {}

        input_data_names = set(self.root_nodes)
        # FIXME: for this particula case, the error here should be TypeError,
        # not KeyError (the former is the one used when calling functions with
        # invalid arguments) - maybe an argument validate.keys to choose
        # which error to raise?
        validate.keys(
            valid=input_data_names,
            passed=set(input_data),
            required=input_data_names,
            name="input_data",
        )

        if copy is True:
            copying_function = copy_module.copy
        elif callable(copy):
            copying_function = copy
        else:
            copying_function = _do_nothing

        for task_name in self.dag:
            task = self.dag[task_name]
            params = task.params.to_dict()

            if task_name in self.root_nodes:
                params = {**params, "input_data": input_data[task_name]}

            # replace params with the returned value from upstream tasks
            if "upstream" in params:
                params["upstream"] = {
                    k: copying_function(outs[k]) for k, v in params["upstream"].items()
                }

            params.pop("product", None)

            output = self.return_postprocessor(task.source.primitive(**params))

            if output is None:
                raise ValueError(
                    "All callables in a {} must return a value. "
                    'Callable "{}", from task "{}" returned None'.format(
                        type(self).__name__, task.source.name, task_name
                    )
                )

            outs[task_name] = output

        return outs
