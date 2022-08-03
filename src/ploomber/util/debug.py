import sys

import click

from ploomber.exceptions import DebuggingFinished


# TODO: we should move this logic to PythonCallable.run
def debug_if_exception(callable_, task_name, kwargs=None):
    """
    Drop a debugger session if running callable_() raises an exception,
    otherwise it just returns the value returned by callable_()
    """
    # NOTE: importing it here, otherwise we get a
    # "If you suspect this is an IPython X.Y.Z bug..." message if any exception
    # after the import if an exception happens
    # NOTE: the IPython.terminal.debugger module has pdb-like classes but it
    # doesn't mimic pdb's API exactly, ipdb is just a wrapper that takes care
    # of those details - I tried using IPython directly but bumped into some
    # issues
    import ipdb

    kwargs = kwargs or dict()

    try:
        result = callable_(**kwargs)
    except Exception as e:
        click.secho(f'Error in task {task_name!r}. '
                    'Starting debugger...',
                    fg='red')

        ipdb.post_mortem(sys.exc_info()[2])

        raise DebuggingFinished(task_name) from e
    else:
        return result
