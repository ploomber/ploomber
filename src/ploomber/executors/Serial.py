"""
DAG executors
"""
import logging

import networkx as nx
from tqdm.auto import tqdm
from ploomber.Table import BuildReport
from ploomber.executors.Executor import Executor
from ploomber.executors.LoggerHandler import LoggerHandler


class Serial(Executor):
    """Runs a DAG serially
    """
    TASKS_CAN_CREATE_CHILD_PROCESSES = True
    STOP_ON_EXCEPTION = True

    def __init__(self, logging_directory=None, logging_level=logging.INFO):
        self.logging_directory = logging_directory
        self.logging_level = logging_level
        self._logger = logging.getLogger(__name__)

    def __call__(self, dag, **kwargs):
        if self.logging_directory:
            logger_handler = LoggerHandler(dag_name=dag.name,
                                           directory=self.logging_directory,
                                           logging_level=self.logging_level)
            logger_handler.add()

        status_all = []

        g = dag._to_graph()
        pbar = tqdm(nx.algorithms.topological_sort(g), total=len(g))

        for t in pbar:
            pbar.set_description('Building task "{}"'.format(t.name))

            try:
                t.build(**kwargs)
            except Exception as e:
                if dag._on_task_failure:
                    dag._on_task_failure(t)

                raise e
            else:
                if dag._on_task_finish:
                    dag._on_task_finish(t)

            status_all.append(t.build_report)

        build_report = BuildReport(status_all)
        self._logger.info(' DAG report:\n{}'.format(repr(build_report)))

        for client in dag.clients.values():
            client.close()

        if self.logging_directory:
            logger_handler.remove()

        return build_report

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build
        # them again in __setstate__
        del state['_logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger(__name__)
