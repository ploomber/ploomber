from enum import Enum


class TaskStatus(Enum):
    WaitingRender = 'waiting_render'
    WaitingExecution = 'waiting_execution'
    WaitingUpstream = 'waiting_upstream'
    Executed = 'executed'
    Errored = 'errored'
