from functools import wraps
import sys
import traceback
from ploomber.io import TerminalWriter
from ploomber.exceptions import DAGBuildError, DAGRenderError


def cli_endpoint(fn):
    """
    Decorator for command line endpoints (note: functions decorated with
    this must be called with keyword arguments). It runs the cli endpoint,
    captures exception (if any), sends a colored traceback to standard error
    and exits with code 1. Call some_endpoint(catch_exception=False) to disable
    this behavior (e.g. for testing)
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
