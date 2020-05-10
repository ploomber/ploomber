"""
Serial DAG executor: builds one task at a time
"""
import warnings
from multiprocessing import Pool
import traceback
import logging

from tqdm.auto import tqdm
from ploomber.executors.Executor import Executor
from ploomber.exceptions import DAGBuildError
from ploomber.MessageCollector import MessageCollector
from ploomber.constants import TaskStatus


# TODO: add a SerialIterator executor


class Serial(Executor):
    """Runs a DAG one task at a time

    Tries to run as many tasks as possible (even if some of them fail), when
    tasks fail, a final traceback is shown at the end of the execution showing
    error messages along with their corresponding task to ease debgging, the
    same happens with warnings: they are captured and shown at the end of the
    execution (but only when build_in_subprocess is False).

    Parameters
    ----------
    build_in_subprocess : bool, optional
        Determines whether tasks should be executed in a subprocess or in the
        current process. For pipelines with a lot of PythonCallables loading
        large objects such as pandas.DataFrame, this option is recommended as
        it guarantees that memory will be cleared up upon task execution.
        Defaults to True

    """
    # TODO: maybe add a parameter: stop on first exception, same for Parallel

    def __init__(self, build_in_subprocess=True):
        self._logger = logging.getLogger(__name__)
        self._build_in_subprocess = build_in_subprocess

    def __call__(self, dag, show_progress, task_kwargs):
        super().__call__(dag)

        exceptions = MessageCollector()
        warnings_ = MessageCollector()
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
                with warnings.catch_warnings(record=True) as warnings_current:
                    if (callable(t.source.primitive)
                            and self._build_in_subprocess):
                        report = execute_in_subprocess(t, task_kwargs)
                    else:
                        report = t.build(**task_kwargs)
            except Exception:
                t.exec_status = TaskStatus.Errored
                new_status = TaskStatus.Errored
                tr = traceback.format_exc()
                exceptions.append(message=tr, task_str=repr(t))
            else:
                new_status = TaskStatus.Executed

                try:
                    t.exec_status = new_status
                except Exception:
                    tr = traceback.format_exc()
                    exceptions.append(message=tr, task_str=repr(t))

                task_reports.append(report)

            if warnings_current:
                w = [str(a_warning.message) for a_warning
                     in warnings_current]
                warnings_.append(task_str=t.name,
                                 message='\n'.join(w))

        # end of for loop

        if warnings_:
            # FIXME: maybe raise one by one to keep the warning type
            warnings.warn('Some tasks had warnings when rendering DAG '
                          '"{}":\n{}'.format(dag.name, str(warnings_)))

        if exceptions:
            raise DAGBuildError('DAG build failed, the following '
                                'tasks crashed '
                                '(corresponding downstream tasks aborted '
                                'execution):\n{}'
                                .format(str(exceptions)))

        # only close when tasks are executed in this process (otherwise
        # this won't have any effect anyway)
        if not self._build_in_subprocess:
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
