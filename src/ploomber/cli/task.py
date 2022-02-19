from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.telemetry import telemetry

# TODO: we are just smoke testing this, we need to improve the tests
# (check the appropriate functions are called)


@cli_endpoint
@telemetry.log_call('task')
def main():
    parser = CustomParser(description='Build tasks', prog='ploomber task')
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
                            help='Force task build (ignore up-to-date status)',
                            action='store_true')
        parser.add_argument('--status',
                            '-st',
                            help='Get task status',
                            action='store_true')
        parser.add_argument('--on-finish',
                            '-of',
                            help='Only execute on_finish hook',
                            action='store_true')
    dag, args = parser.load_from_entry_point_arg()

    dag.render()
    task = dag[args.task_name]

    if args.source:
        print(task.source)

    if args.status:
        print(task.status())

    if args.on_finish:
        task._run_on_finish()

    # task if build by default, but when --source or --status are passed,
    # the --build flag is required
    no_flags = not any((args.build, args.status, args.source, args.on_finish))

    if no_flags or args.build:
        task.build(force=args.force)
