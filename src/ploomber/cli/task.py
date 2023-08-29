from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.telemetry import telemetry
from ploomber.cloud.api import PloomberCloudAPI
from ploomber.tasks import NotebookRunner, PythonCallable
from ploomber.executors import _format
from ploomber.messagecollector import task_build_exception
from ploomber.exceptions import TaskBuildError
import click

# TODO: we are just smoke testing this, we need to improve the tests
# (check the appropriate functions are called)

ONLY_IN_CALLABLES_AND_NBS = "Only supported in function and notebook tasks."


def _task_cli(accept_task_id=False):
    parser = CustomParser(description="Build tasks", prog="ploomber task")

    with parser:
        parser.add_argument("task_name")
        parser.add_argument(
            "--source", "-s", help="Print task source code", action="store_true"
        )
        parser.add_argument(
            "--build",
            "-b",
            help="Build task (default if no other option is passed)",
            action="store_true",
        )
        parser.add_argument(
            "--force",
            "-f",
            help="Force task build (ignore up-to-date status)",
            action="store_true",
        )
        parser.add_argument(
            "--status", "-st", help="Get task status", action="store_true"
        )
        parser.add_argument(
            "--on-finish",
            "-of",
            help="Only execute on_finish hook",
            action="store_true",
        )
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--debug",
            "-d",
            help=("Start debugger upon crashing. " + ONLY_IN_CALLABLES_AND_NBS),
            action="store_true",
        )
        group.add_argument(
            "--debuglater",
            "-D",
            help=(
                "Serialize traceback for later debugging. " + ONLY_IN_CALLABLES_AND_NBS
            ),
            action="store_true",
        )

        if accept_task_id:
            parser.add_argument("--task-id")

    dag, args = parser.load_from_entry_point_arg()

    dag.render()
    task = dag[args.task_name]

    if args.source:
        print(task.source)

    if args.status:
        print(task.status())

    if args.on_finish:
        task._run_on_finish()

    if args.debug:
        debug_mode = "now"
    elif args.debuglater:
        debug_mode = "later"
    else:
        # no debug
        debug_mode = None

    # TODO: support debug in python callable
    if isinstance(task, (NotebookRunner, PythonCallable)):
        task.debug_mode = debug_mode

    # task if build by default, but when --source or --status are passed,
    # the --build flag is required
    no_flags = not any((args.build, args.status, args.source, args.on_finish))

    err = None

    if no_flags or args.build:
        try:
            task.build(force=args.force)
        except Exception as e:
            err = e

        if err is not None:
            if getattr(args, "task_id", None):
                PloomberCloudAPI().tasks_update(getattr(args, "task_id"), "failed")

            msg = _format.exception(err)
            exception_string = task_build_exception(
                task=task, message=msg, exception=err
            )

            raise TaskBuildError(exception_string)
        else:
            click.echo(f"{task.name!r} task executed successfully!")
            click.echo("Products:\n" + repr(task.product))

            if getattr(args, "task_id", None):
                PloomberCloudAPI().tasks_update(getattr(args, "task_id"), "finished")


@cli_endpoint
@telemetry.log_call("task")
def main():
    _task_cli()


# TODO: test this
if __name__ == "__main__":
    _task_cli(accept_task_id=True)
