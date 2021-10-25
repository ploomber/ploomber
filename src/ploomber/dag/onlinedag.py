import pickle
try:
    import importlib.resources as importlib_resources
except ImportError:  # pragma: no cover
    # backported
    import importlib_resources
import abc
from pathlib import Path
from itertools import chain

from ploomber import InMemoryDAG, DAG
from ploomber.spec.dagspec import DAGSpecPartial
from ploomber.tasks import input_data_passer, in_memory_callable


class OnlineDAG(abc.ABC):
    """
    Execute partial DAGs in-memory. This is an abstract class, to use it.
    Create a subclass and provide the required static methods.

    See here for a complete example:
    https://github.com/ploomber/projects/blob/master/templates/ml-online/src/ml_online/infer.py
    """

    # FIXME: add a way to customize
    def __init__(self):
        dag = self.init_dag_from_partial(self.get_partial())

        # TODO: add support for manually specifying upstream dependencies
        upstream = {
            name: dag[name].source.extract_upstream()
            for name in dag._iter()
        }

        # names of all tasks used as upstream
        upstream_tasks = chain(*upstream.values())

        # find tasks that are declared as upstream but do not exist in the dag
        missing = set(upstream_tasks) - set(dag)

        for name in missing:
            input_data_passer(dag, name=name)

        # TODO: maybe delete all upstream dependencies and set them again
        # (raise a warning if there are some upstream dependencies?)
        # this doesn't happen when we get a yaml file because we control
        # that using extract_upstream=False but might happen if we receive
        # a DAG object already
        # the dag is complete now, set all upstream dependencies
        for name in dag._iter():
            for dependency in upstream.get(name, []):
                dag[name].set_upstream(dag[dependency])

        # get all terminal nodes and make them a dependency of the  node
        terminal_current = [
            name for name, degree in dag._G.out_degree() if not degree
        ]

        # TODO: extract upstream and make sure they match with the ones in
        # terminal_current
        terminal = in_memory_callable(self.terminal_task,
                                      dag,
                                      name='terminal',
                                      params=self.terminal_params())

        for dependency in terminal_current:
            terminal.set_upstream(dag[dependency])

        self.in_memory = InMemoryDAG(dag)

    @classmethod
    def init_dag_from_partial(cls, partial):
        """Initialize partial returned by get_partial()
        """
        if isinstance(partial, (str, Path)):
            spec = DAGSpecPartial(partial)
            return spec.to_dag()
        elif isinstance(partial, DAG):
            return partial
        else:
            raise TypeError(f'Expected {cls.__name__}.get_partial() to '
                            'return a str, pathlib.Path or ploomber.DAG, '
                            f'got {type(partial).__name__}')

    def predict(self, **kwargs):
        """
        Run the DAG

        Parameters
        ----------
        **kwargs
            One parameter per root task (task with no upstream dependencies)
            in the partial DAG.

        Returns
        -------
        A dictionary with {task_name: returned_value}
        """
        return self.in_memory.build(kwargs)

    @abc.abstractstaticmethod
    def get_partial():
        """
        Must return the location of a partial dag (str or pathlib.Path)
        """
        pass

    @abc.abstractstaticmethod
    def terminal_task(upstream, model):
        """
        Las function to execute. The ``upstream`` parameter contains the
        output of all tasks that have no downstream dependencies
        """
        pass

    @abc.abstractstaticmethod
    def terminal_params():
        """
        Must return a dictionary with parameters passed to ``terminal_task``
        """
        pass


class OnlineModel(OnlineDAG):
    """
    A subclass of :py:mod:`ploomber.OnlineDAG` to provider a simpler interface
    for online DAGs whose terminal task calls ``model.predict``.
    ``OnlineModel`` is initialized with a module following a standard
    structure. Looks for a ``pipeline-features.yaml`` in the module's
    root directory (e.g. ``src/my_module/pipeline-features.yaml``),
    a ``model.pickle`` in the module's root directory. The terminal task
    is executed with a ``model`` parameter which contains the load model
    and calls ``model.predict``. The last task in ``pipeline-features.yaml``
    should be named ``features``.

    See here for a complete example:
    https://github.com/ploomber/projects/blob/master/templates/ml-online/src/ml_online/infer.py

    Parameters
    ----------
    module
        A module following a standard structure

    Examples
    --------
    >>> import my_module
    >>> model = OnlineModel(my_module)
    >>> model.predict(x=some_input)
    """
    def __init__(self, module):
        self._module = module
        super().__init__()

        names = list(self.in_memory.dag._iter())

        if 'features' not in names:
            raise ValueError(
                f'Error initializing {type(self).__name__}. The initialized '
                'DAG must contain a task named "features". '
                f'Got tasks with names: {names}')

    def get_partial(self):
        with importlib_resources.path(
                self._module, 'pipeline-features.yaml') as path_to_spec:
            path = path_to_spec

        return path

    def terminal_params(self):
        error = None

        try:
            content = importlib_resources.read_binary(self._module,
                                                      'model.pickle')
        except FileNotFoundError as e:
            error = e

        if error:
            raise FileNotFoundError(
                f'Error initializing {type(self).__name__} '
                f'from module {self._module.__name__!r}: Missing model file. '
                f'Expected location: {error.filename}')

        model = pickle.loads(content)
        return dict(model=model)

    @staticmethod
    def terminal_task(upstream, model):
        return model.predict(upstream['features'])

    def predict(self, **kwargs):
        """
        Returns the output of ``model.predict(upstream['features'])``
        """
        return self.in_memory.build(kwargs)['terminal']
