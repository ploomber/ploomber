from ploomber.entry.parsers import _custom_command, CustomParser


def main():
    parser = CustomParser(description='Get task information')
    parser.add_argument('entry_point', help='Entry point (DAG)')
    parser.add_argument('task_name')
    parser.add_argument('--source',
                        '-s',
                        help='Print task source code',
                        action='store_true')
    parser.add_argument('--build',
                        '-b',
                        help='Build task',
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

    if args.build:
        task.build(force=args.force)
