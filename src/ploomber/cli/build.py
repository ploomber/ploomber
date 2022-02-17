import sys

from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.executors import Parallel
from ploomber.telemetry import telemetry


# this parameter is only set to True when calling "ploomber interactive"
@cli_endpoint
@telemetry.log_call('build', payload=True)
def main(payload, render_only=False):
    parser = CustomParser(description='Build pipeline', prog='ploomber build')

    with parser:
        parser.add_argument('--force',
                            '-f',
                            help='Force execution by ignoring status',
                            action='store_true',
                            default=False)
        parser.add_argument('--skip-upstream',
                            '-su',
                            help='Skip building upstream dependencies. '
                            'Only applicable when using --partially',
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

    # users may try to run "ploomber build {name}" to build a single task
    if len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
        suggestion = 'ploomber task {task-name}'
        cmd_name = parser.prog
        telemetry.log_api("unsupported_build_cmd",
                          metadata={
                              'cmd_name': cmd_name,
                              'suggestion': suggestion,
                              'argv': sys.argv
                          })
        parser.error(f'{cmd_name!r} does not take positional arguments.\n'
                     f'To build a single task, try: {suggestion!r}')

    dag, args = parser.load_from_entry_point_arg()

    # when using the parallel executor from the CLI, ensure we print progress
    # to stdout
    if isinstance(dag.executor, Parallel):
        dag.executor.print_progress = True

    if render_only:
        dag.render()
    else:
        if args.partially:
            report = dag.build_partially(args.partially,
                                         force=args.force,
                                         debug=args.debug,
                                         skip_upstream=args.skip_upstream)
        else:
            report = dag.build(force=args.force, debug=args.debug)

        if report:
            print(report)

    payload['dag'] = dag

    return dag
