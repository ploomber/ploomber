from functools import wraps
import sys
import traceback
from ploomber.io import TerminalWriter
from ploomber.exceptions import DAGBuildError, DAGRenderError


def cli_endpoint(fn):
    """
    Runs a function, capturing an exception (if any) and displaying a colored
    traceback
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            fn(*args, **kwargs)
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
                tw = TerminalWriter()
                tw._write_source(error.splitlines())
            else:
                print(error)

            sys.exit(1)

    return wrapper
