from enum import Enum


# TODO: rename to use the name wors as the hooks: finished and failure
class DAGStatus(Enum):
    WaitingRender = 'waiting_render'
    WaitingExecution = 'waiting_execution'
    ErroredRender = 'errored_render'
    Executed = 'executed'
    Errored = 'errored'


# TODO: rename to use the name wors as the hooks: finished and failure
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
    Skipped = 'skipped'
    # crashed
    Errored = 'errored'
    # an upstream dependency crashed
    Aborted = 'aborted'

    BrokenProcessPool = 'broken_process_pool'
