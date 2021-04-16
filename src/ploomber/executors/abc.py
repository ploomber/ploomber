import abc

from ploomber.constants import TaskStatus


class Executor(abc.ABC):
    """
    Abstract class for executors.

    *Executors API is subject to change*

    Executors handle task execution, they receive a dag as a parameter and
    call task.build() on each task. To iterate over tasks, it is recommended
    that they use dag.values() as it returns tasks in topological order.

    Tasks self-report its execution status (Executed/Errored) but if the
    executor runs it in a different process, it is responsible for reporting
    the status change back to the task object, as this change triggers
    changes in status in dowsmtream tasks (e.g. if a task fails, downstream
    dependencies are aborted)

    It is safe to skip task.build() on tasks that are either Skipped or
    Aborted.

    Although not strictly required, it is recommended for executors to capture
    all warnings and exceptions, then display a summary at the end of the
    execution, showing each task with their corresponding errors and warnings.

    Executors do not run hooks, these are triggered by the DAG object
    (DAG-level hooks) and Task objects (task-level hooks). These translates
    DAG hooks to be executed in the main process and Task hooks in the same
    process where Task._build() is called, this is an important detail for
    executors that run tasks in different processes, they have to report
    back the returned value from  Task._build and assign it to the
    corresponding copy of the Task in the main process. Upon sucessful
    execution, metadata is cleared up (FIXME: we should really be sending new
    metadata instead of clearing it to force a new fetch).

    Runnning tasks and task hooks in subprocesses has the advantage ensuring
    memory is cleared after the task finishes.

    To allow finishing dag.build() gracefully, executors should raise
    DAGBuildError (this will trigger the DAG.on_failure hook)


    Notes
    -----
    The following is still being defined: do we need to send the whole dag
    object? Looks like we are good by just sending the tasks
    """
    @abc.abstractmethod
    def __call__(self, dag):
        exec_status = set([t.exec_status for t in dag.values()])

        if exec_status - {
                TaskStatus.WaitingExecution,
                TaskStatus.WaitingUpstream,
                TaskStatus.Skipped,
                TaskStatus.WaitingDownload,
        }:
            raise ValueError('Tasks should only have either '
                             'TaskStatus.WaitingExecution or '
                             'TaskStatus.WaitingUpstream before attempting '
                             'to execute, got status: {}'.format(exec_status))
