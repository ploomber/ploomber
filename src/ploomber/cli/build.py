from ploomber.cli.parsers import _custom_command, CustomParser
from ploomber.cli.io import cli_endpoint


# this parameter is only set to True when calling "ploomber interactive"
@cli_endpoint
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

    if render_only:
        dag.render()
    else:
        if args.partially:
            report = dag.build_partially(args.partially,
                                         force=args.force,
                                         debug=args.debug)
        else:
            report = dag.build(force=args.force, debug=args.debug)

        if report:
            print(report)

    return dag
