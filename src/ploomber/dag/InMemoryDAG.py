import copy as copy_module

from ploomber.util import validate
from ploomber.tasks import PythonCallable


def _do_nothing(x):
    return x


class InMemoryDAG:
    """
    Converts a DAG to a DAG-like object that performs all operations in memory
    (products are not serialized). For this to work all tasks must be
    PythonCallable initialized with callables that return a value

    Parameters
    ----------
    dag : ploomber.DAG
        The DAG to use
    """
    def __init__(self, dag):
        types = {type(dag[t]) for t in dag._iter()}

        if not {PythonCallable} >= types:
            extra = ','.join(
                [type_.__name__ for type_ in types - {PythonCallable}])
            raise TypeError('All tasks in the DAG must be PythonCallable, '
                            'got unallowed types: {}'.format(extra))

        dag.render()

        self.dag = dag
        self.root_nodes = [
            name for name, degree in dag._G.in_degree() if not degree
        ]

    def build(self, root_params, copy=False):
        """Run the DAG

        Parameters
        ----------
        root_params : dict
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

        root_params_names = set(self.root_nodes)
        validate.keys(valid=root_params_names,
                      passed=set(root_params),
                      required=root_params_names,
                      name='root_params')

        if copy is True:
            copying_function = copy_module.copy
        elif callable(copy):
            copying_function = copy
        else:
            copying_function = _do_nothing

        for task_name in self.dag:
            task = self.dag[task_name]
            params = task.params.to_dict()

            # if root node, replace input params (but do not replace upstream
            # not product)
            if task_name in self.root_nodes:
                passed_params = root_params[task_name] or {}
                params = {**params, **passed_params}

            # replace params with output
            if 'upstream' in params:
                params['upstream'] = {
                    k: copying_function(outs[k])
                    for k, v in params['upstream'].items()
                }

            output = task.source.primitive(**params)

            if output is None:
                raise ValueError(
                    'All callables in a {} must return a value. '
                    'Callable "{}", from task "{}" returned None'.format(
                        type(self).__name__, task.source.name, task_name))

            outs[task_name] = output

        return outs
