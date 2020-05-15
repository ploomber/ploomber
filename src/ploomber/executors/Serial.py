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

    def __init__(self, build_in_subprocess=True, catch_exceptions=True,
                 catch_warnings=True):
        self._logger = logging.getLogger(__name__)
        self._build_in_subprocess = build_in_subprocess
        self._catch_exceptions = catch_exceptions
        self._catch_warnings = catch_warnings

    def __call__(self, dag, show_progress, task_kwargs):
        super().__call__(dag)

        exceptions_all = MessageCollector()
        warnings_all = MessageCollector()
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

            if self._build_in_subprocess:
                fn = LazyFunction(build_in_subprocess,
                                  {'task': t, 'build_kwargs': task_kwargs,
                                   'reports_all': task_reports},
                                  t)
            else:
                fn = LazyFunction(build_in_current_process,
                                  {'task': t, 'build_kwargs': task_kwargs,
                                   'reports_all': task_reports},
                                  t)

            if self._catch_exceptions:
                fn = LazyFunction(fn=catch_warnings,
                                  kwargs={'fn': fn,
                                          'warnings_all': warnings_all},
                                  task=t)

            if self._catch_exceptions:
                fn = LazyFunction(fn=catch_exceptions,
                                  kwargs={'fn': fn,
                                          'exceptions_all': exceptions_all},
                                  task=t)

            fn()

        # end of for loop

        if warnings_all and self._catch_warnings:
            # FIXME: maybe raise one by one to keep the warning type
            warnings.warn('Some tasks had warnings when executing DAG '
                          '"{}":\n{}'.format(dag.name, str(warnings_all)))

        if exceptions_all and self._catch_exceptions:
            raise DAGBuildError('DAG build failed, the following '
                                'tasks crashed '
                                '(corresponding downstream tasks aborted '
                                'execution):\n{}'
                                .format(str(exceptions_all)))

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


class LazyFunction:
    def __init__(self, fn, kwargs, task):
        self.fn = fn
        self.kwargs = kwargs
        self.task = task

    def __call__(self):
        return self.fn(**self.kwargs)


def catch_warnings(fn, warnings_all):
    with warnings.catch_warnings(record=True) as warnings_current:
        result = fn()

    if warnings_current:
        w = [str(a_warning.message) for a_warning
             in warnings_current]
        warnings_all.append(task_str=fn.task.name,
                            message='\n'.join(w))

    return result


def catch_exceptions(fn, exceptions_all):
    try:
        fn()
    except Exception:
        fn.task.exec_status = TaskStatus.Errored
        new_status = TaskStatus.Errored
        tr = traceback.format_exc()
        exceptions_all.append(message=tr, task_str=repr(fn.task))
    else:
        new_status = TaskStatus.Executed

        try:
            fn.task.exec_status = new_status
        except Exception:
            tr = traceback.format_exc()
            exceptions_all.append(message=tr, task_str=repr(fn.task))


def build_in_current_process(task, build_kwargs, reports_all):
    report = task.build(**build_kwargs)
    reports_all.append(report)


def build_in_subprocess(task, build_kwargs, reports_all):
    if callable(task.source.primitive):
        p = Pool(processes=1)
        res = p.apply_async(func=task.build, kwds=build_kwargs)
        # calling this make sure we catch the exception, from the docs:
        # Return the result when it arrives. If timeout is not None and
        # the result does not arrive within timeout seconds then
        # multiprocessing.TimeoutError is raised. If the remote call
        # raised an exception then that exception will be reraised by
        # get().
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.AsyncResult.get
        report = res.get()
        p.close()
        p.join()
    else:
        report = task.build(**build_kwargs)

    reports_all.append(report)
