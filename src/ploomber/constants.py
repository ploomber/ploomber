from enum import Enum


# TODO: rename to use the names as the hooks: finished and failure
class DAGStatus(Enum):
    WaitingRender = 'waiting_render'
    WaitingExecution = 'waiting_execution'
    ErroredRender = 'errored_render'
    Executed = 'executed'
    Errored = 'errored'


# TODO: rename to use the names as the hooks: finished and failure
class TaskStatus(Enum):
    # INIT

    # waiting render
    WaitingRender = 'waiting_render'

    # SET AT RENDERING TIME
    # At rendering time: traverse in topological order and determine
    # status by checking downstream dependencies at the end of Task.render

    # up-to-date, no need to run. Propagate to move downstream dependencies
    # from WaitingUpstream to WaitingExecution
    # TODO: rename to UpToDate
    Skipped = 'skipped'

    # outdated: waiting to be run by the executor
    WaitingExecution = 'waiting_execution'
    # outdated: waits for upstream dependencies to be either Executed
    # or Skipped
    WaitingUpstream = 'waiting_upstream'
    # up-to-date but need to download from remote location
    WaitingDownload = 'waiting_download'

    # failed to render, propagate ErroredRender to downstream dependencies
    # downstream dependencies and set them to AbortedRender
    ErroredRender = 'errored_render'
    # Am I really stopping render due to this?
    AbortedRender = 'aborted_render'

    # SET AT BUILDING TIME
    # Two options, one is similar to the process at rendering time: determine
    # status when entering Task.build and by checking downstream dependencies
    # Second option: propagate on task.exec_status = TaskStatus.Status
    # this is less efficient but has the advantage that we could do "live
    # monitoring" as changes in status are always updated

    # succesfully executed, propagate to move downstream dependencies from
    # WaitingUpstream to WaitingExecution
    Executed = 'executed'

    # crashed, propagate to move downstream dependencies from WaitingUpstream
    # to Aborted
    Errored = 'errored'

    # an upstream dependency crashed, do not execute, propagate to abort
    # any downstream dependency
    Aborted = 'aborted'

    BrokenProcessPool = 'broken_process_pool'
