"""
Serial DAG executor: builds one task at a time
"""
from multiprocessing import Pool
import traceback
import logging

from tqdm.auto import tqdm
from ploomber.executors.Executor import Executor
from ploomber.executors.LoggerHandler import LoggerHandler
from ploomber.exceptions import DAGBuildError
from ploomber.ExceptionCollector import ExceptionCollector
from ploomber.constants import TaskStatus


# TODO: add a SerialIterator executor


class Serial(Executor):
    """Runs a DAG serially

    """
    # TODO: maybe add a parameter: stop on first exception, same for Parallel

    def __init__(self, logging_directory=None, logging_level=logging.INFO,
                 execute_callables_in_subprocess=True):
        self.logging_directory = logging_directory
        self.logging_level = logging_level
        self._logger = logging.getLogger(__name__)
        self._execute_callables_in_subprocess = execute_callables_in_subprocess

    def __call__(self, dag, show_progress, task_kwargs):
        super().__call__(dag)

        if self.logging_directory:
            logger_handler = LoggerHandler(dag_name=dag.name,
                                           directory=self.logging_directory,
                                           logging_level=self.logging_level)
            logger_handler.add()

        exceptions = ExceptionCollector()
        task_reports = []

        if show_progress:
            tasks = tqdm(dag.values(), total=len(dag))
        else:
            tasks = dag.values()

        for t in tasks:
            if t.exec_status in {TaskStatus.Skipped, TaskStatus.Aborted}:
                continue

            if show_progress:
                tasks.set_description('Building task "{}"'.format(t.name))

            try:
                if (callable(t.source.value)
                        and self._execute_callables_in_subprocess):
                    report = execute_in_subprocess(t, task_kwargs)
                else:
                    report = t.build(**task_kwargs)
            except Exception:
                t.exec_status = TaskStatus.Errored
                new_status = TaskStatus.Errored
                tr = traceback.format_exc()
                exceptions.append(traceback_str=tr, task_str=repr(t))
            else:
                new_status = TaskStatus.Executed

                try:
                    t.exec_status = new_status
                except Exception:
                    tr = traceback.format_exc()
                    exceptions.append(traceback_str=tr, task_str=repr(t))

                task_reports.append(report)

        if exceptions:
            raise DAGBuildError('DAG build failed, the following '
                                'tasks crashed '
                                '(corresponding downstream tasks aborted '
                                'execution):\n{}'
                                .format(str(exceptions)))

        if self.logging_directory:
            logger_handler.remove()

        # only close when tasks are executed in this process (otherwise
        # this won't have any effect anyway)
        if not self._execute_callables_in_subprocess:
            for client in dag.clients.values():
                client.close()

        return task_reports

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build
        # them again in __setstate__
        del state['_logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger(__name__)


def execute_in_subprocess(task, build_kwargs):
    p = Pool(processes=1)
    res = p.apply_async(func=task.build, kwds=build_kwargs)
    # calling this make sure we catch the exception, from the docs:
    # Return the result when it arrives. If timeout is not None and
    # the result does not arrive within timeout seconds then
    # multiprocessing.TimeoutError is raised. If the remote call
    # raised an exception then that exception will be reraised by
    # get().
    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.AsyncResult.get
    result = res.get()
    p.close()
    p.join()
    return result
