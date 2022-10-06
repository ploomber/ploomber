"""
Task.exec_status only makes sense for parallel execution, when using the serial
executor, we can update the _status automatically (within Task) but it is
unusable, get rid of the logic that updates it automatically, however,
we ar eusing here the _update status here to propagate dowstream updates.
So think about which parts can go away and which ones should not
"""
import os
import logging
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from ploomber.constants import TaskStatus
from ploomber.executors.abc import Executor
from ploomber.exceptions import DAGBuildError
from ploomber.messagecollector import (BuildExceptionsCollector, Message)
from ploomber.executors import _format
from multiprocessing import get_context, get_start_method
from ploomber.io import pretty_print

from tqdm.auto import tqdm
import click

# TODO: support for show_progress, we can use a progress bar but we have
# to modify the label since at any point more than one task might be
# executing

# TODO: add support for logging errors, then add this executor to the tests
# in test_hooks


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
            return output
        except Exception as e:
            return Message(task=self.task, message=_format.exception(e), obj=e)


def _log(msg, logger, print_progress):
    logger(msg)

    if print_progress:
        print(msg)


class Parallel(Executor):
    """Runs a DAG in parallel using multiprocessing

    Parameters
    ----------
    processes : int, default=None
        The number of processes to use. If None, uses ``os.cpu_count``

    print_progress : bool, default=False
        Whether to print progress to stdout, otherwise just log it

    start_method : str, default=None
        The method which should be used to start child processes. method
        can be 'fork', 'spawn' or 'forkserver'. If None or empty then the
        default start_method is used.

    Examples
    --------
    Spec API:

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        # add at the top of your pipeline.yaml
        executor: parallel

        tasks:
          - source: script.py
            nb_product_key: [nb_ipynb, nb_html]
            product:
                nb_ipynb: nb.ipynb
                nb_html: report.html


    Python API:

    >>> from ploomber import DAG
    >>> from ploomber.executors import Parallel
    >>> dag = DAG(executor='parallel') # use with default values
    >>> dag = DAG(executor=Parallel(processes=2)) # customize


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

    Notes
    -----
    If any task crashes, downstream tasks execution is aborted, building
    continues until no more tasks can be executed

    .. versionadded:: 0.20
        Added `start_method` argument

    See Also
    --------
    ploomber.executors.Serial :
        Serial executor

    """

    # NOTE: Tasks should not create child processes
    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process.daemon
    multiprocessing_start_methods = ['fork', 'spawn', 'forkserver']

    def __init__(self,
                 processes=None,
                 print_progress=False,
                 start_method=None):
        self.processes = processes or os.cpu_count()
        self.print_progress = print_progress

        self._logger = logging.getLogger(__name__)
        self._i = 0
        start_method = start_method or get_start_method()
        self._bar = None
        if start_method in self.multiprocessing_start_methods:
            self.start_method = start_method
        else:
            raise ValueError(
                'Invalid start_method. '
                f'Valid values for start_method are: '
                f'{pretty_print.iterable(self.multiprocessing_start_methods)}')

    def __call__(self, dag, show_progress):
        super().__call__(dag)

        # TODO: Have to test this with other Tasks, especially the ones that
        # use clients - have to make sure they are serialized correctly
        done = []
        started = []
        set_all = set(dag)
        future_mapping = {}

        # there might be up-to-date tasks, add them to done
        # FIXME: this only happens when the dag is already build and then
        # then try to build again (in the same session), if the session
        # is restarted even up-to-date tasks will be WaitingExecution again
        # this is a bit confusing, so maybe change WaitingExecution
        # to WaitingBuild?
        n_scheduled = 0

        for name in dag:
            if dag[name].exec_status in {
                    TaskStatus.Executed, TaskStatus.Skipped
            }:
                done.append(dag[name])
            else:
                n_scheduled += 1

        if show_progress:
            self._bar = tqdm(total=n_scheduled)
        else:
            self._bar = None

        def callback(future):
            """Keep track of finished tasks
            """
            if self._bar:
                self._bar.update()

            task = future_mapping[future]

            self._logger.debug('Added %s to the list of finished tasks...',
                               task.name)
            try:
                result = future.result()
            except BrokenProcessPool:
                # ignore the error here but flag the task,
                # so next_task is able to stop the iteration,
                # when we call result after breaking the loop,
                # this will show up
                task.exec_status = TaskStatus.BrokenProcessPool
                if self._bar:
                    self._bar.colour = 'red'
            else:
                if isinstance(result, Message):
                    task.exec_status = TaskStatus.Errored
                    if self._bar:
                        self._bar.colour = 'red'
                # sucessfully run task._build
                else:
                    # ignore report here, we just the metadata to update it
                    _, meta = result
                    task.product.metadata.update_locally(meta)
                    task.exec_status = TaskStatus.Executed

            done.append(task)

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
                if (task.exec_status in {
                        TaskStatus.WaitingExecution, TaskStatus.WaitingDownload
                } and task not in started):
                    return task
                # there might be some up-to-date tasks, add them

            set_done = set([t.name for t in done])

            if not self._i % 50000:
                click.clear()

                if set_done:
                    _log(f'Finished: {pretty_print.iterable(set_done)}',
                         self._logger.debug,
                         print_progress=self.print_progress)

                remaining = pretty_print.iterable(set_all - set_done)
                _log(f'Remaining: {remaining}',
                     self._logger.debug,
                     print_progress=self.print_progress)
                _log(f'Finished {len(set_done)} out of {len(set_all)} tasks',
                     self._logger.info,
                     print_progress=self.print_progress)

            if set_done == set_all:
                self._logger.debug('All tasks done')

                raise StopIteration

            self._i += 1

        task_kwargs = {'catch_exceptions': True}

        context = get_context(self.start_method)

        with ProcessPoolExecutor(max_workers=self.processes,
                                 mp_context=context) as pool:
            while True:
                try:
                    task = next_task()
                except StopIteration:
                    break
                else:
                    if task is not None:
                        future = pool.submit(TaskBuildWrapper(task),
                                             **task_kwargs)
                        # the callback function uses the future mapping
                        # so add it before registering the callback, otherwise
                        # it might break and hang the whole process
                        future_mapping[future] = task
                        future.add_done_callback(callback)
                        started.append(task)
                        self._logger.info('Added %s to the pool...', task.name)

        results = [
            # results are the output of Task._build: (report, metadata)
            # OR a Message
            get_future_result(f, future_mapping)
            for f in future_mapping.keys()
        ]

        exps = [r for r in results if isinstance(r, Message)]

        if self._bar:
            self._bar.close()

        if exps:
            raise DAGBuildError(str(BuildExceptionsCollector(exps)))

        # if we reach this, it means no tasks failed. only return reports
        return [r[0] for r in results]

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build
        # them again in __setstate__
        del state['_logger']
        # the progress bar is not pickable
        del state['_bar']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger(__name__)
        self._bar = None


def get_future_result(future, future_mapping):
    try:
        return future.result()
    except BrokenProcessPool as e:
        raise BrokenProcessPool('Broken pool {}'.format(
            future_mapping[future])) from e
