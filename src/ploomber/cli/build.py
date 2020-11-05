from functools import partial
import sys

from ploomber.cli.parsers import _custom_command, CustomParser
from ploomber.executors import Serial


# this parameter is only set to True when calling "ploomber interactive"
def main(render_only=False):
    parser = CustomParser(description='Build pipeline')

    with parser:
        parser.add_argument('--force',
                            '-f',
                            help='Force execution by ignoring status',
                            action='store_true',
                            default=False)
        parser.add_argument(
            '--partially',
            '-p',
            help='Build a pipeline partially until certain task',
            default=None)
        parser.add_argument(
            '--debug',
            '-d',
            help='Drop a debugger session if an exception happens',
            action='store_true',
            default=False)

    dag, args = _custom_command(parser)

    # if debug, we have to change the executor to these settings, if we run
    # tasks in a subprocess or catch exception, we won't be able to start the
    # debugging session in the right place
    if args.debug:
        dag.executor = Serial(build_in_subprocess=False,
                              catch_exceptions=False,
                              catch_warnings=False)

    if render_only:
        dag.render()
    else:
        if args.partially:
            callable_ = partial(dag.build_partially,
                                args.partially,
                                force=args.force)
        else:
            callable_ = partial(dag.build, force=args.force)

        raise Exception

        if args.debug:
            report = debug_mode(callable_)
        else:
            report = callable_()

        if report:
            print(report)

    return dag


def debug_mode(callable_):
    # NOTE: importing it here, otherwise we get a
    # "If you suspect this is an IPython X.Y.Z bug..." message if any exception
    # after the import if an exception happens
    # NOTE: the IPython.terminal.debugger module has pdb-like classes but it
    # doesn't mimic pdb's API exactly, ipdb is just a wrapper that takes care
    # of those details - I tried using IPython directly but bumped into some
    # issues
    # should we implement in DAG, so we can do dag.build(debug=True)?
    import ipdb

    try:
        report = callable_()
    except Exception:
        ipdb.post_mortem(sys.exc_info()[2])
    finally:
        report = None

    return report
