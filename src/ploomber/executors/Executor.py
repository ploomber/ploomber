from ploomber.constants import TaskStatus


class Executor:
    def __call__(self, dag):
        exec_status = set([t.exec_status for t in dag.values()])

        if exec_status - {TaskStatus.WaitingExecution,
                          TaskStatus.WaitingUpstream}:
            raise ValueError('Tasks should only have either '
                             'TaskStatus.WaitingExecution or '
                             'TaskStatus.WaitingUpstream before attempting '
                             'to execute, got status: {}'
                             .format(exec_status))
