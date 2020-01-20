import logging
from multiprocessing import Pool
from ploomber.constants import TaskStatus
from ploomber.executors.Executor import Executor
from ploomber.executors.LoggerHandler import LoggerHandler


class Parallel(Executor):
    """Runs a DAG in parallel using the multiprocessing module
    """
    # Tasks should not create child processes, see documention:
    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process.daemon
    TASKS_CAN_CREATE_CHILD_PROCESSES = False
    STOP_ON_EXCEPTION = False

    def __init__(self, processes=4, logging_directory=None,
                 logging_level=logging.INFO):
        self.logging_directory = logging_directory
        self.logging_level = logging_level
        self.processes = processes

        self._logger = logging.getLogger(__name__)
        self._i = 0

    def __call__(self, dag, **kwargs):
        if self.logging_directory:
            logger_handler = LoggerHandler(dag_name=dag.name,
                                           directory=self.logging_directory,
                                           logging_level=self.logging_level)
            logger_handler.add()
        # TODO: Have to test this with other Tasks, especially the ones that use
        # clients - have to make sure they are serialized correctly
        done = []
        started = []
        set_all = set(dag)

        # there might be up-to-date tasks, add them to done
        # FIXME: this only happens when the dag is already build and then
        # then try to build again (in the same session), if the session
        # is restarted even up-to-date tasks will be WaitingExecution again
        # this is a bit confusing, so maybe change WaitingExecution
        # to WaitingBuild?
        for name in dag:
            if dag[name]._status == TaskStatus.Executed:
                done.append(dag[name])

        def callback(task):
            """Keep track of finished tasks
            """
            self._logger.debug('Added %s to the list of finished tasks...',
                               task.name)
            done.append(task)

        def next_task():
            """
            Return the next Task to execute, returns None if no Tasks are available
            for execution (cause their dependencies are not done yet) and raises
            a StopIteration exception if there are no more tasks to run, which means
            the DAG is done
            """
            # update task status for tasks in the done list
            for task in done:
                task = dag[task.name]
                # TODO: must check for esxecution status - if there is an error
                # is this status updated automatically? cause it will be better
                # for tasks to update their status by themselves, then have a
                # manager to update the other tasks statuses whenever one finishes
                # to know which ones are available for execution
                task._status = TaskStatus.Executed

                # update other tasks status, should abstract this in a execution
                # manager, also make the _get_downstream more efficient by
                # using the networkx data structure directly
                for t in task._get_downstream():
                    t._update_status()

            # iterate over tasks to find which is ready for execution
            for task_name in dag:
                # ignore tasks that are already started, I should probably add an
                # executing status but that cannot exist in the task itself,
                # maybe in the manaer?
                if (dag[task_name]._status == TaskStatus.WaitingExecution
                        and dag[task_name] not in started):
                    t = dag[task_name]
                    return t
                # there might be some up-to-date tasks, add them

            # if all tasks are done, stop
            set_done = set([t.name for t in done])

            if not self._i % 1000:
                self._logger.debug('Finished tasks so far: %s', set_done)
                self._logger.debug('Remaining tasks: %s',
                                   set_all - set_done)
                self._logger.info('Finished %i out of %i tasks',
                                  len(set_done), len(set_all))

            if set_done == set_all:
                self._logger.debug('All tasks done')

                if self.logging_directory:
                    logger_handler.remove()

                raise StopIteration

            self._i += 1

        with Pool(processes=self.processes) as pool:
            while True:
                try:
                    task = next_task()
                except StopIteration:
                    break
                else:
                    if task is not None:
                        res = pool.apply_async(
                            task.build, [], kwds=kwargs, callback=callback)
                        started.append(task)
                        logging.info('Added %s to the pool...', task.name)
                        # time.sleep(3)

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
