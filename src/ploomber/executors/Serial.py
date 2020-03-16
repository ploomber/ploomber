"""
DAG executors
"""
from datetime import datetime
from multiprocessing import Pool
import traceback
import logging

from tqdm.auto import tqdm
from ploomber.Table import BuildReport, Row
from ploomber.executors.Executor import Executor
from ploomber.executors.LoggerHandler import LoggerHandler
from ploomber.exceptions import DAGBuildError, TaskBuildError
from ploomber.ExceptionCollector import ExceptionCollector
from ploomber.constants import TaskStatus


# TODO: add a SerialIterator executor
# TODO: document how executors should handle errors, they need to catch
# failures in task.build but also in t.exec_status = TaskStatus.Executed
# to report on_finish errors


class Serial(Executor):
    """Runs a DAG serially
    """
    # TODO: maybe add a parameter: stop on first exception, same for Parallel
    # TODO: add option to run all tasks in a subprocess
    def __init__(self, logging_directory=None, logging_level=logging.INFO,
                 execute_callables_in_subprocess=True):
        self.logging_directory = logging_directory
        self.logging_level = logging_level
        self._logger = logging.getLogger(__name__)
        self._execute_callables_in_subprocess = execute_callables_in_subprocess

    def __call__(self, dag, **kwargs):
        super().__call__(dag)

        if self.logging_directory:
            logger_handler = LoggerHandler(dag_name=dag.name,
                                           directory=self.logging_directory,
                                           logging_level=self.logging_level)
            logger_handler.add()

        exceptions = ExceptionCollector()
        task_reports = []

        pbar = tqdm(dag._topologically_sorted_iter(skip_aborted=True),
                    total=len(dag))

        for t in pbar:
            pbar.set_description('Building task "{}"'.format(t.name))

            then = datetime.now()

            try:
                if (callable(t.source.value)
                        and self._execute_callables_in_subprocess):
                    new_status = execute_in_subprocess(t, kwargs)
                else:
                    new_status = t.build(**kwargs)
            except Exception as e:
                t.exec_status = TaskStatus.Errored
                new_status = TaskStatus.Errored
                tr = traceback.format_exc()
                exceptions.append(traceback_str=tr, task_str=repr(t))

                # FIXME: this should not be here, but called
                # inside the task, or the dag, depending on the level
                if dag._on_task_failure:
                    dag._on_task_failure(t)
            else:
                try:
                    t.exec_status = new_status
                except Exception as e:
                    tr = traceback.format_exc()
                    exceptions.append(traceback_str=tr, task_str=repr(t))

                if dag._on_task_finish:
                    dag._on_task_finish(t)
            finally:
                now = datetime.now()
                did_execute = new_status == TaskStatus.Executed
                elapsed = (now - then).total_seconds()
                report = {'name': t.name,
                          'Ran?': did_execute,
                          'Elapsed (s)': elapsed if did_execute else 0}
                task_reports.append(Row(report))

        if exceptions:
            raise DAGBuildError('DAG build failed, the following '
                                'tasks crashed '
                                '(corresponding downstream tasks aborted '
                                'execution):\n{}'
                                .format(str(exceptions)))

        build_report = BuildReport(task_reports)

        self._logger.info(' DAG report:\n{}'.format(repr(build_report)))

        # TODO: this should be moved to the superclass, should be like
        # a cleanup function, add a test to verify that this happens
        # even if execution fails
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
