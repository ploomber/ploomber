from enum import Enum


class DAGStatus(Enum):
    WaitingRender = 'waiting_render'
    WaitingExecution = 'waiting_execution'
    ErroredRender = 'errored_render'
    Executed = 'executed'
    Errored = 'errored'


class TaskStatus(Enum):
    # waiting render
    WaitingRender = 'waiting_render'
    # waiting to be started
    WaitingExecution = 'waiting_execution'
    # waiting for upstream dependencies to finish
    WaitingUpstream = 'waiting_upstream'
    # failed to render
    ErroredRender = 'errored_render'
    AbortedRender = 'aborted_render'
    # succesfully executed
    Executed = 'executed'
    # crashed
    Errored = 'errored'
    # an upstream dependency crashed
    Aborted = 'aborted'
