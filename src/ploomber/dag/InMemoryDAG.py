import copy as copy_module


def _do_nothing(x):
    return x


class InMemoryDAG:
    """
    Converts a DAG to a DAG-like object that performs all operations in memory
    (products are not serialized). For this to work all tasks must be
    PythonCallable and the sources for them must return a value

    Parameters
    ----------
    dag : ploomber.DAG
        The DAG to use
    """
    def __init__(self, dag):
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

        extra = set(root_params) - set(self.root_nodes)
        missing = set(self.root_nodes) - set(root_params)

        if missing or extra:
            raise ValueError

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
                params = {**params, **root_params[task_name]}

            # replace params with output
            if 'upstream' in params:
                params['upstream'] = {
                    k: copying_function(outs[k])
                    for k, v in params['upstream'].items()
                }

            outs[task_name] = task.source.primitive(**params)

        return outs
