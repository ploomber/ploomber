import traceback

from ploomber.exceptions import TaskBuildError, RenderError, TaskRenderError

from papermill.exceptions import PapermillExecutionError


def exception(exc):
    """Formats an exception into a more concise traceback

    Parameters
    ----------
    """
    exceptions = [exc]

    while exc.__cause__:
        exceptions.append(exc.__cause__)
        exc = exc.__cause__

    exceptions.reverse()

    breakpoint = None

    for i, exc in enumerate(exceptions):
        if isinstance(exc, (TaskBuildError)):
            breakpoint = i
            break

    exc_hide = exceptions[breakpoint:]

    if breakpoint is not None:
        tr = _format_exception(exceptions[breakpoint - 1])
        tr = tr + '\n' + '\n'.join(f'{_get_exc_name(exc)}: {str(exc)}'
                                   for exc in exc_hide)
    else:
        # if not breakpoint, take the outermost exception and show it.
        # this ensures we show the full traceback in case there are chained
        # exceptions
        tr = _format_exception(exceptions[-1])

    return tr


def _get_exc_name(exc):
    return f'{exc.__module__}.{type(exc).__name__}'


def _format_exception(exc):
    if isinstance(exc,
                  (PapermillExecutionError, RenderError, TaskRenderError)):
        tr = str(exc)
    else:
        tr = ''.join(
            traceback.format_exception(type(exc),
                                       exc,
                                       exc.__traceback__,
                                       limit=None))

    return tr
