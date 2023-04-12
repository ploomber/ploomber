import traceback

from ploomber.exceptions import TaskBuildError, RenderError, TaskRenderError

from papermill.exceptions import PapermillExecutionError


def exception(exc):
    """Formats an exception into a more concise traceback

    Parameters
    ----------
    """

    # extract all the exception objects, in case this is a chained exception
    exceptions = [exc]

    while exc.__cause__:
        exceptions.append(exc.__cause__)
        exc = exc.__cause__

    # reverse to get the most specific error first
    exceptions.reverse()

    # find the first instance of TaskBuildError
    breakpoint = None

    for i, exc in enumerate(exceptions):
        if isinstance(exc, (TaskBuildError)):
            breakpoint = i
            break

    # using the breakpoint, find the exception where we'll only display the
    # error message, not the traceback. That is, the first TaskBuildError
    # exception
    exc_short = exceptions[breakpoint:]

    if breakpoint is not None:
        # this happens when running a single task, all exception can be
        # TaskBuildError
        if breakpoint != 0:
            # traceback info applies to non-TaskBuildError
            # (this takes care of chained exceptions as well)
            tr = _format_exception(exceptions[breakpoint - 1])
        else:
            tr = ""

        # append the short exceptions (only error message)
        tr = (
            tr
            + "\n"
            + "\n".join(f"{_get_exc_name(exc)}: {str(exc)}" for exc in exc_short)
        )
    else:
        # if not breakpoint, take the outermost exception and show it.
        # this ensures we show the full traceback in case there are chained
        # exceptions
        tr = _format_exception(exceptions[-1])

    return tr


def _get_exc_name(exc):
    return f"{exc.__module__}.{type(exc).__name__}"


def _format_exception(exc):
    # for these ones, the traceback isn't important, so just display the
    # message
    if isinstance(exc, (PapermillExecutionError, RenderError, TaskRenderError)):
        tr = str(exc)
    else:
        # format the exception, this will take care of chained exceptions
        # as well
        tr = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__, limit=None)
        )

    return tr
