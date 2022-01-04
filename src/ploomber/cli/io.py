from functools import wraps
import sys
import traceback
from ploomber.io import TerminalWriter
from ploomber.exceptions import DAGBuildError, DAGRenderError

# TODO: there are two types of cli commands: the ones that execute user's
# code (ploomber build/task) and the ones that parse a dag/task but do not
# execute it. For the former, we want to capture errors and display them with
# colors so it's easier for the user to understand what went wrong with their
# code. For the latter, the errors are raise by us, hence, we only need to
# print the message and exit. Currently, all CLI end points (except ploomber
# nb) are decorated with @cli_endpoint but we should change it to
# @command_endpoint


def cli_endpoint(fn):
    """
    Decorator for command line endpoints that execute dags or tasks. It runs
    the decorated function, captures exception (if any), sends a colored
    traceback to standard error and exits with code 1.

    Notes
    -----
    Functions decorated with this must be called with keyword arguments

    Call some_endpoint(catch_exception=False) to disable this behavior (e.g.
    for testing)
    """
    @wraps(fn)
    def wrapper(catch_exception=True, **kwargs):
        if catch_exception:
            try:
                fn(**kwargs)
            # these already color output
            except (DAGBuildError, DAGRenderError):
                error = traceback.format_exc()
                color = False
            except Exception:
                error = traceback.format_exc()
                color = True
            else:
                error = None

            if error:
                if color:
                    tw = TerminalWriter(file=sys.stderr)
                    tw._write_source(error.splitlines())
                else:
                    print(error, file=sys.stderr)

                sys.exit(1)
        else:
            fn(**kwargs)

    return wrapper


# FIXME: capture only certain types of exceptions. If it's something we dind't
# raise, we'd like to see the full traceback
def command_endpoint(fn):
    """
    Decorator for command line endpoints that only parse dags or tasks but do
    not execute them. If it tails, it prints error message to stderror, then
    calls with exit code 1.
    """
    @wraps(fn)
    def wrapper(**kwargs):
        try:
            fn(**kwargs)
        except Exception as e:
            print(f'Error: {e}', file=sys.stderr)
            sys.exit(1)

    return wrapper
