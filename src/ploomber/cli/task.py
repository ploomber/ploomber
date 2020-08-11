from ploomber.cli.parsers import _custom_command, CustomParser


def main():
    parser = CustomParser(description='Get task information')
    with parser:
        parser.add_argument('task_name')
        parser.add_argument('--source',
                            '-s',
                            help='Print task source code',
                            action='store_true')
        parser.add_argument(
            '--build',
            '-b',
            help='Build task (default if no other option is passed)',
            action='store_true')
        parser.add_argument('--force',
                            '-f',
                            help='Force build task (ignore up-to-date status)',
                            action='store_true')
        parser.add_argument('--status',
                            '-st',
                            help='Get task status',
                            action='store_true')
    dag, args = _custom_command(parser)

    dag.render(force=args.force)
    task = dag[args.task_name]

    if args.source:
        print(task.source)

    if args.status:
        print(task.status())

    # task if built by default, but when --source or --status are passed,
    # the --build flag is required
    no_flags = not any((args.build, args.status, args.source))
    if no_flags or args.build:
        task.build(force=args.force)
