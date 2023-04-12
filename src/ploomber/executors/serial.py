import warnings
from multiprocessing import Pool
import logging

from tqdm.auto import tqdm
from ploomber.executors.abc import Executor
from ploomber.executors import _format
from ploomber.exceptions import DAGBuildError, DAGBuildEarlyStop
from ploomber.messagecollector import BuildExceptionsCollector, BuildWarningsCollector
from ploomber.constants import TaskStatus


class Serial(Executor):
    """Executor than runs one task at a time

    Parameters
    ----------
    build_in_subprocess : bool, optional
        Determines whether tasks should be executed in a subprocess or in the
        current process. For pipelines with a lot of PythonCallables loading
        large objects such as pandas.DataFrame, this option is recommended as
        it guarantees that memory will be cleared up upon task execution.
        Defaults to True.

    catch_exceptions : bool, optional
        Whether to catch exceptions raised when building tasks and running
        hooks. If True, exceptions are collected and displayed at the end,
        downstream tasks of failed ones are aborted (not executed at all),
        If any task raises a DAGBuildEarlyStop exception, the final exception
        raised will be of such type. If False, no catching is done, on_failure
        won't be executed and task status will not be updated and tracebacks
        from build and hooks won't be logged. Setting of to False is only
        useful for debugging purposes.

    catch_warnings : bool, optional
        If True, the executor catches all warnings raised by tasks and
        displays them at the end of execution. If catch_exceptions is True
        and there is an error building the DAG, capture warnings are still
        shown before raising the collected exceptions.

    Examples
    --------
    Spec API:

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        # add at the top of your pipeline.yaml
        executor: serial

        tasks:
          - source: script.py
            nb_product_key: [nb_ipynb, nb_html]
            product:
                nb_ipynb: nb.ipynb
                nb_html: report.html


    Python API:

    >>> from ploomber import DAG
    >>> from ploomber.executors import Serial
    >>> dag = DAG(executor='parallel') # use with default values
    >>> dag = DAG(executor=Serial(build_in_subprocess=False)) # customize


    DAG can exit gracefully on function tasks (PythonCallable):

    >>> from ploomber.products import File
    >>> from ploomber.tasks import PythonCallable
    >>> from ploomber.exceptions import DAGBuildEarlyStop
    >>> # A PythonCallable function that raises DAGBuildEarlyStop
    >>> def early_stop_root(product):
    ...     raise DAGBuildEarlyStop('Ending gracefully')

    >>> # Since DAGBuildEarlyStop is raised, DAG will exit gracefully.
    >>> dag = DAG(executor='parallel')
    >>> t = PythonCallable(early_stop_root, File('file.txt'), dag=dag)
    >>> dag.build()  # doctest: +SKIP


    DAG can also exit gracefully on notebook tasks:

    >>> from pathlib import Path
    >>> from ploomber.tasks import NotebookRunner
    >>> from ploomber.products import File
    >>> def early_stop():
    ...     raise DAGBuildEarlyStop('Ending gracefully')

    >>> # Use task-level hook "on_finish" to exit DAG gracefully.
    >>> dag = DAG(executor='parallel')
    >>> t = NotebookRunner(Path('nb.ipynb'), File('report.html'), dag=dag)
    >>> t.on_finish = early_stop
    >>> dag.build()  # doctest: +SKIP

    See Also
    --------
    ploomber.executors.Parallel :
        Parallel executor
    """

    def __init__(
        self, build_in_subprocess=True, catch_exceptions=True, catch_warnings=True
    ):
        self._logger = logging.getLogger(__name__)
        self._build_in_subprocess = build_in_subprocess
        self._catch_exceptions = catch_exceptions
        self._catch_warnings = catch_warnings

    def __repr__(self):
        return (
            "Serial(build_in_subprocess={}, "
            "catch_exceptions={}, catch_warnings={})".format(
                self._build_in_subprocess, self._catch_exceptions, self._catch_warnings
            )
        )

    def __call__(self, dag, show_progress):
        super().__call__(dag)

        exceptions_all = BuildExceptionsCollector()
        warnings_all = BuildWarningsCollector()
        task_reports = []

        task_kwargs = {"catch_exceptions": self._catch_exceptions}

        scheduled = [dag[t] for t in dag if dag[t].exec_status != TaskStatus.Skipped]

        if show_progress:
            scheduled = tqdm(scheduled, total=len(scheduled))

        for t in scheduled:
            if t.exec_status == TaskStatus.Aborted:
                continue

            if show_progress:
                label = (
                    "Building task"
                    if t.exec_status == TaskStatus.WaitingExecution
                    else "Downloading product"
                )
                scheduled.set_description(f"{label} {t.name!r}")

            if self._build_in_subprocess:
                fn = LazyFunction(
                    build_in_subprocess,
                    {
                        "task": t,
                        "build_kwargs": task_kwargs,
                        "reports_all": task_reports,
                    },
                    t,
                )
            else:
                fn = LazyFunction(
                    build_in_current_process,
                    {
                        "task": t,
                        "build_kwargs": task_kwargs,
                        "reports_all": task_reports,
                    },
                    t,
                )

            if self._catch_warnings:
                fn = LazyFunction(
                    fn=catch_warnings,
                    kwargs={"fn": fn, "warnings_all": warnings_all},
                    task=t,
                )
            else:
                # NOTE: this isn't doing anything
                fn = LazyFunction(fn=pass_exceptions, kwargs={"fn": fn}, task=t)

            if self._catch_exceptions:
                fn = LazyFunction(
                    fn=catch_exceptions,
                    kwargs={"fn": fn, "exceptions_all": exceptions_all},
                    task=t,
                )

            fn()

        # end of for loop

        if warnings_all and self._catch_warnings:
            # NOTE: maybe raise one by one to keep the warning type
            warnings.warn(str(warnings_all))

        if exceptions_all and self._catch_exceptions:
            early_stop = any(
                [isinstance(m.obj, DAGBuildEarlyStop) for m in exceptions_all]
            )
            if early_stop:
                raise DAGBuildEarlyStop(
                    "Ealy stopping DAG execution, "
                    "at least one of the tasks that "
                    "failed raised a DAGBuildEarlyStop "
                    "exception:\n{}".format(str(exceptions_all))
                )
            else:
                raise DAGBuildError(str(exceptions_all))

        return task_reports

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_logger"]
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
    """
    Catch all warnings on the current task (except DeprecationWarning)
    and append them to warnings_all. Runs if the parameter catch_warnings
    is true.

    Parameters
    ----------
    fn : function
        A LazyFunction that automatically calls catch_warnings, with parameters
        from the main function (warnings_all) and the current scheduled task.

    warnings_all: BuildWarningsCollector object
        Collects all warnings.
    """
    # TODO: we need a try catch in case fn() raises an exception
    with warnings.catch_warnings(record=True) as warnings_current:
        warnings.simplefilter("ignore", DeprecationWarning)

        result = fn()

    if warnings_current:
        w = [str(a_warning.message) for a_warning in warnings_current]
        warnings_all.append(task=fn.task, message="\n".join(w))

    return result


def catch_exceptions(fn, exceptions_all):
    """
    If there is an exception, log it and append it to warnings_all. Runs if
    the parameter catch_exceptions is true.

    Parameters
    ----------
    fn : function
        A LazyFunction that automatically calls catch_exceptions, with
        parameters from the main function (exceptions_all) and the
        current scheduled task.

    exceptions_all: BuildExceptionsCollector object
        Collects all exceptions.
    """
    logger = logging.getLogger(__name__)
    # NOTE: we are individually catching exceptions
    # (see build_in_current_process and build_in_subprocess), would it be
    # better to catch everything here and set
    # task.exec_status = TaskStatus.Errored accordingly?

    # TODO: setting exec_status can also raise exceptions if the hook fails
    # add tests for that, and check the final task status,
    try:
        # try to run task build
        fn()
    except Exception as e:
        # if running in a different process, logger.exception inside Task.build
        # won't show up. So we do it here.
        # FIXME: this is going to cause duplicates if not running in a
        # subprocess
        logger.exception(str(e))
        tr = _format.exception(e)
        exceptions_all.append(task=fn.task, message=tr, obj=e)


def pass_exceptions(fn):
    """
    Pass all exceptions for the current task and nothing. Runs if
    both parameters catch_exceptions and catch_warnings are false.

    Parameters
    ----------
    fn : function
        A LazyFunction that automatically calls pass_exceptions on the
        current scheduled task.
    """
    # should i still check here for DAGBuildEarlyStop? is it worth
    # for returning accurate task status?
    fn()


def build_in_current_process(task, build_kwargs, reports_all):
    """
    Execute the current task in the current process. Runs if the parameter
    build_in_subprocess is false.

    Parameters
    ----------
    task : DAG object
        The current task.

    build_kwargs: dict
        Contains bool catch_exceptions and bool catch_warnings, checks
        whether to catch exceptions and warnings on the current task.

    reports_all: list
        Collects the build report when executing the current DAG.
    """
    report, meta = task._build(**build_kwargs)
    reports_all.append(report)


def build_in_subprocess(task, build_kwargs, reports_all):
    """
    Execute the current task in a subprocess. Runs if the parameter
    build_in_subprocess is true.

    Parameters
    ----------
    task : DAG object
        The current task.

    build_kwargs: dict
        Contains bool catch_exceptions and bool catch_warnings, checks
        whether to catch exceptions and warnings on the current task.

    reports_all: list
        Collects the build report when executing the current DAG.
    """
    if callable(task.source.primitive):
        try:
            p = Pool(processes=1)
        except RuntimeError as e:
            if "An attempt has been made to start a new process" in str(e):
                # this is most likely due to child processes created with
                # spawn (mac/windows) outside if __name__ == '__main__'
                raise RuntimeError(
                    "Press ctrl + c to exit. "
                    "For help solving this, go to: "
                    "https://ploomber.io/s/mp"
                ) from e
            else:
                raise

        res = p.apply_async(func=task._build, kwds=build_kwargs)
        # calling this make sure we catch the exception, from the docs:
        # Return the result when it arrives. If timeout is not None and
        # the result does not arrive within timeout seconds then
        # multiprocessing.TimeoutError is raised. If the remote call
        # raised an exception then that exception will be reraised by
        # get().
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.AsyncResult.get
        try:
            report, meta = res.get()
        except Exception:
            # we have to update status since this is ran in a subprocess
            task.exec_status = TaskStatus.Errored
            raise
        else:
            task.product.metadata.update_locally(meta)
            task.exec_status = TaskStatus.Executed
            reports_all.append(report)
        finally:
            p.close()
            p.join()

    else:
        # we run other tasks in the same process
        report, meta = task._build(**build_kwargs)
        task.product.metadata.update_locally(meta)
        reports_all.append(report)
