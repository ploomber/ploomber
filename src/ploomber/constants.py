from enum import Enum


class DAGStatus(Enum):
    WaitingRender = 'waiting_render'
    WaitingExecution = 'waiting_execution'
    Executed = 'executed'
    Errored = 'errored'


class TaskStatus(Enum):
    # waiting render
    WaitingRender = 'waiting_render'
    # waiting to be started
    WaitingExecution = 'waiting_execution'
    # waiting for upstream dependencies to finish
    WaitingUpstream = 'waiting_upstream'
    # succesfully executed
    Executed = 'executed'
    # crashed
    Errored = 'errored'
    # an upstream dependency crashed
    Aborted = 'aborted'
