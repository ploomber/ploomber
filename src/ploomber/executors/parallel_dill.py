# optional dependency
try:
    from multiprocess import Pool
except ModuleNotFoundError:
    Pool = None

import os
import logging

from tqdm.auto import tqdm

from ploomber.constants import TaskStatus
from ploomber.executors.abc import Executor
from ploomber.exceptions import DAGBuildError
from ploomber.messagecollector import BuildExceptionsCollector, Message
from ploomber.executors import _format
from ploomber.io import pretty_print
from ploomber_core.dependencies import requires

import click


class TaskBuildWrapper:
    """
    Wraps task._build. Traceback is lost when using ProcessPoolExecutor
    (future.exception().__traceback__ returns None), so we have to catch it
    here and return it so at the end of the DAG execution, a message with the
    traceback can be printed
    """

    def __init__(self, task):
        self.task = task

    def __call__(self, *args, **kwargs):
        try:
            output = self.task._build(**kwargs)
            return output, self.task.name
        except Exception as e:
            e._name = self.task.name
            raise


def _log(msg, logger, print_progress):
    logger(msg)

    if print_progress:
        print(msg)


class ParallelDill(Executor):
    """
    Runs a DAG in parallel, serializes using dill, which allows declaring
    and running pipelines in a Jupyter notebook.

    Parameters
    ----------
    processes : int, default=None
        The number of processes to use. If None, uses ``os.cpu_count``

    print_progress : bool, default=False
        Whether to print progress to stdout, otherwise just log it

    Notes
    -----
    If any task crashes, downstream tasks execution is aborted, building
    continues until no more tasks can be executed

    See Also
    --------
    ploomber.executors.Serial :
        Serial executor

    ploomber.executors.Parallel :
        Parallel executor
    """

    @requires(["multiprocess"], "ParallelDill")
    def __init__(self, processes=None, print_progress=False):
        self.processes = processes or os.cpu_count()
        self.print_progress = print_progress

        self._logger = logging.getLogger(__name__)
        self._i = 0
        self._bar = None

    def __call__(self, dag, show_progress):
        super().__call__(dag)

        done = []
        started = []
        failed = []
        set_all = set(dag)
        future_mapping = {}
        self._dag = dag

        n_scheduled = 0

        for name in dag:
            if dag[name].exec_status in {TaskStatus.Executed, TaskStatus.Skipped}:
                done.append(dag[name])
            else:
                n_scheduled += 1

        if show_progress:
            self._bar = tqdm(total=n_scheduled)
        else:
            self._bar = None

        def callback(future):
            """Keep track of finished tasks"""
            if self._bar:
                self._bar.update()

            result, task_name = future

            task = self._dag[task_name]
            self._logger.debug("Added %s to the list of finished tasks...", task.name)

            if isinstance(result, Message):
                task.exec_status = TaskStatus.Errored

                if self._bar:
                    self._bar.colour = "red"

            # sucessfully run task._build
            else:
                # ignore report here, we just the metadata to update it
                _, meta = result
                task.product.metadata.update_locally(meta)
                task.exec_status = TaskStatus.Executed

            done.append(task)
            started.remove(task)

        def callback_error(exc):
            if self._bar:
                self._bar.update()

            # TODO: if some unknown exception occurs, exc wont have ._name
            task = self._dag[exc._name]
            self._logger.debug("Added %s to the list of finished tasks...", task.name)

            task.exec_status = TaskStatus.Errored

            if self._bar:
                self._bar.colour = "red"

            done.append(task)
            started.remove(task)

            failed.append(Message(task=task, message=_format.exception(exc), obj=exc))

        def next_task():
            """
            Return the next Task to execute, returns None if no Tasks are
            available for execution (cause their dependencies are not done yet)
            and raises a StopIteration exception if there are no more tasks to
            run, which means the DAG is done
            """
            for task in dag.values():
                if task.exec_status in {TaskStatus.Aborted}:
                    if self._bar:
                        self._bar.update()

                    done.append(task)
                elif task.exec_status == TaskStatus.BrokenProcessPool:
                    raise StopIteration

            # iterate over tasks to find which is ready for execution
            for task in dag.values():
                # ignore tasks that are already started, I should probably add
                # an executing status but that cannot exist in the task itself,
                # maybe in the manaer?
                if (
                    task.exec_status
                    in {TaskStatus.WaitingExecution, TaskStatus.WaitingDownload}
                    and task not in started
                ):
                    return task
                # there might be some up-to-date tasks, add them

            set_done = set([t.name for t in done])

            if not self._i % 50000:
                click.clear()

                if set_done:
                    _log(
                        f"Finished: {pretty_print.iterable(set_done)}",
                        self._logger.debug,
                        print_progress=self.print_progress,
                    )

                remaining = pretty_print.iterable(set_all - set_done)
                _log(
                    f"Remaining: {remaining}",
                    self._logger.debug,
                    print_progress=self.print_progress,
                )
                _log(
                    f"Finished {len(set_done)} out of {len(set_all)} tasks",
                    self._logger.info,
                    print_progress=self.print_progress,
                )

            if set_done == set_all:
                self._logger.debug("All tasks done")

                raise StopIteration

            self._i += 1

        task_kwargs = {"catch_exceptions": True}

        with Pool(processes=self.processes) as pool:
            while True:
                wait = True
                while wait:
                    current = [t.name for t in started]
                    wait = len(current) >= self.processes

                if self._bar:
                    current_ = pretty_print.iterable(current)
                    self._bar.set_description(f"Running: {current_}")

                try:
                    task = next_task()
                except StopIteration:
                    break
                else:
                    if task is not None:
                        async_result = pool.apply_async(
                            TaskBuildWrapper(task),
                            kwds=task_kwargs,
                            callback=callback,
                            error_callback=callback_error,
                        )
                        future_mapping[async_result] = task
                        started.append(task)
                        self._logger.info("Added %s to the pool...", task.name)

        if self._bar:
            self._bar.close()

        if failed:
            raise DAGBuildError(str(BuildExceptionsCollector(failed)))

        results = [f.get()[0] for f in future_mapping.keys()]

        return [r[0] for r in results]

    # same as Parallel (and possibly Executor?)
    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build
        # them again in __setstate__
        del state["_logger"]
        # progress bar is not picklable
        del state["_bar"]
        return state

    # same as Parallel (and possibly Executor?)
    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger(__name__)
        self._bar = None
