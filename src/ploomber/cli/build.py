from ploomber.cli.parsers import _custom_command, CustomParser


def main():
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)')
    parser.add_argument('--action',
                        '-a',
                        help='Action to execute, defaults to '
                        'build',
                        default='build')
    parser.add_argument('--force',
                        '-f',
                        help='Force execution by ignoring status',
                        action='store_true',
                        default=False)
    # TODO: should ignore action
    parser.add_argument('--partially',
                        '-p',
                        help='Build a pipeline partially until certain task',
                        default=None)

    dag, args = _custom_command(parser)

    kwargs = {'force': args.force}

    if args.partially:
        dag.build_partially(args.partially, **kwargs)
    else:
        print(getattr(dag, args.action)(**kwargs))

    return dag
