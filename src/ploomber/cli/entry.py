from ploomber.cli.parsers import _custom_command, CustomParser


def _main():
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)')
    parser.add_argument('--action',
                        help='Action to execute, defaults to '
                        'build',
                        default='build')
    # TODO: should ignore action
    parser.add_argument('--partially',
                        '-p',
                        help='Build a pipeline partially until certain task',
                        default=None)

    dag, args = _custom_command(parser)

    if args.partially:
        dag.build_partially(args.partially)
    else:
        print(getattr(dag, args.action)())

    return dag
