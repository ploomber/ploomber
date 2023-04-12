import os
from functools import wraps
import sys

try:
    # optional dependency
    import ipdb as pdb
except ModuleNotFoundError:
    import pdb

import click

from ploomber.io import TerminalWriter
from ploomber.exceptions import DAGBuildError, DAGRenderError
from ploomber_core.exceptions import BaseException
from ploomber.executors import _format

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
    This will hide the traceback when raising subclasses of
    ploomber.exeptions.BaseException. To display the traceback, set the
    PLOOMBER_DEBUG variable to true. To start a post-mortem session, set
    PLOOMBER_POST_MORTEM to true.

    Functions decorated with this must be called with keyword arguments

    Call some_endpoint(catch_exception=False) to disable this behavior (e.g.
    for testing)
    """

    @wraps(fn)
    def wrapper(catch_exception=True, **kwargs):
        if os.environ.get("PLOOMBER_DEBUG"):
            catch_exception = False

        if catch_exception:
            try:
                fn(**kwargs)
            # these already color output
            except (DAGBuildError, DAGRenderError) as e:
                error = str(e)
                color = False
            # for base exceptions (we raise this), we display the message
            # in red (no traceback since it's irrelevant for the user)
            except BaseException as e:
                click.secho(e.get_message(), file=sys.stderr, fg="red")
                sys.exit(1)
            # this means it's an unknown error (either a bug in ploomber or
            # an error in the user's code). we display the full traceback,
            # but still hide irrelevant tracebacks (i.e. if a nested exception
            # is raised where some of the exceptions are TaskBuildError)
            except Exception as e:
                error = _format.exception(e)
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
            if os.environ.get("PLOOMBER_POST_MORTEM"):
                try:
                    fn(**kwargs)
                except Exception:
                    _, _, tb = sys.exc_info()
                    pdb.post_mortem(tb)
            else:
                fn(**kwargs)

    return wrapper


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
        # echo error message when it's a subclass Exception
        except BaseException as e:
            click.secho(e.get_message(), file=sys.stderr, fg="red")
            sys.exit(1)
        # show the full traceback if it's not a subclass Exception
        except Exception as e:
            error = _format.exception(e)  # get the traceback
            if error:
                tw = TerminalWriter(
                    file=sys.stderr
                )  # write to terminal all the traceback
                tw._write_source(error.splitlines())
            else:
                print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    return wrapper
