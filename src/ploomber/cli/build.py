from ploomber.cli.parsers import _custom_command, CustomParser


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

    dag, args = _custom_command(parser)

    if render_only:
        dag.render()
    else:
        if args.partially:
            report = dag.build_partially(args.partially, force=args.force)
        else:
            report = dag.build(force=args.force)

        print(report)

    return dag
