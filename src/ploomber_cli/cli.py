from pathlib import Path
import os
from difflib import get_close_matches
import sys

from ploomber_scaffold import scaffold as scaffold_project
import click

CLICK_VERSION = int(click.__version__[0])
# NOTE: package_name was introduced in version 8
VERSION_KWARGS = dict(package_name="ploomber") if CLICK_VERSION >= 8 else dict()


def _suggest_command(name: str, options):
    if not name or name in options:
        return None

    name = name.lower()

    mapping = {
        "run": "build",
        "execute": "build",
    }

    if name in mapping:
        return mapping[name]

    close_commands = get_close_matches(name, options)

    if close_commands:
        return close_commands[0]
    else:
        return None


@click.group()
@click.version_option(**VERSION_KWARGS)
def cli():
    """
    Ploomber

    Need help? https://ploomber.io/community

    Download an example to the mypipeline/ directory:

    $ ploomber examples -n templates/ml-basic -o mypipeline

    Create a new project in a myproject/ directory:

    $ ploomber scaffold myproject

    """
    pass  # pragma: no cover


@cli.command()
@click.option(
    "--conda/--pip",
    "-c/-p",
    is_flag=True,
    default=None,
    help="Use environment.yaml/requirements.txt for dependencies",
)
@click.option(
    "--package",
    "-P",
    is_flag=True,
    help="Use package template (creates setup.py)",
)
@click.option(
    "--empty",
    "-E",
    is_flag=True,
    help="Create a pipeline.yaml with no tasks",
)
@click.option(
    "--entry-point",
    "-e",
    default=None,
    help="Entry point to add tasks. Invalid if other flags present",
)
@click.argument("name", required=False)
def scaffold(name, conda, package, entry_point, empty):
    """
    Create a new project and task source files

    Step 1. Create new project

      $ ploomber scaffold myproject

      $ cd myproject

    Step 2. Add tasks to the pipeline.yaml file

    Step 3. Create source files

      $ ploomber scaffold

    Need help? https://ploomber.io/community
    """
    from ploomber import scaffold as _scaffold
    from ploomber.telemetry import telemetry

    template = "-e/--entry-point is not compatible with {flag}"
    user_passed_name = name is not None

    if entry_point and name:
        err = '-e/--entry-point is not compatible with the "name" argument'
        telemetry.log_api(
            "scaffold_error",
            metadata={"type": "entry_and_name", "exception": err, "argv": sys.argv},
        )
        raise click.ClickException(err)

    if entry_point and conda:
        err = template.format(flag="--conda")
        telemetry.log_api(
            "scaffold_error",
            metadata={
                "type": "entry_and_conda_flag",
                "exception": err,
                "argv": sys.argv,
            },
        )
        raise click.ClickException(err)

    if entry_point and package:
        err = template.format(flag="--package")
        telemetry.log_api(
            "scaffold_error",
            metadata={
                "type": "entry_and_package_flag",
                "exception": err,
                "argv": sys.argv,
            },
        )
        raise click.ClickException(err)

    if entry_point and empty:
        err = template.format(flag="--empty")
        telemetry.log_api(
            "scaffold_error",
            metadata={
                "type": "entry_and_empty_flag",
                "exception": err,
                "argv": sys.argv,
            },
        )
        raise click.ClickException(err)

    # try to load a dag by looking in default places
    if entry_point is None:
        loaded = _scaffold.load_dag()
    else:
        from ploomber.spec import DAGSpec

        try:
            loaded = (
                DAGSpec(entry_point, lazy_import="skip"),
                Path(entry_point).parent,
                Path(entry_point),
            )
        except Exception as e:
            telemetry.log_api(
                "scaffold_error",
                metadata={
                    "type": "dag_load_failed",
                    "exception": str(e),
                    "argv": sys.argv,
                },
            )
            raise click.ClickException(e) from e

    if loaded:
        if user_passed_name:
            click.secho(
                "The 'name' positional argument is "
                "only valid for creating new projects, ignoring...",
                fg="yellow",
            )

        # existing pipeline, add tasks
        spec, _, path_to_spec = loaded
        _scaffold.add(spec, path_to_spec)

        telemetry.log_api(
            "ploomber_scaffold",
            metadata={
                "type": "add_task",
                "argv": sys.argv,
                "dag": loaded,
            },
        )
    else:
        # no pipeline, create base project
        telemetry.log_api(
            "ploomber_scaffold", metadata={"type": "base_project", "argv": sys.argv}
        )
        scaffold_project.cli(
            project_path=name, conda=conda, package=package, empty=empty
        )


@cli.command()
@click.option(
    "--use-lock/--no-use-lock",
    "-l/-L",
    default=None,
    help=(
        "Use lock/regular files. If not present, uses any "
        "of them, prioritizing lock files"
    ),
)
@click.option(
    "--create-env",
    "-e",
    is_flag=True,
    help=("Create a new environment, otherwise install in the " "current environment"),
)
@click.option(
    "--use-venv",
    "-v",
    is_flag=True,
    help="Use Python's venv module (ignoring conda if installed)",
)
def install(use_lock, create_env, use_venv):
    """
    Install dependencies
    """
    from ploomber import cli as cli_module

    cli_module.install.main(use_lock=use_lock, create_env=create_env, use_venv=use_venv)


@cli.command()
@click.option("-n", "--name", help="Example to download", default=None)
@click.option("-f", "--force", help="Force examples download", is_flag=True)
@click.option("-o", "--output", help="Target directory", default=None)
@click.option("-b", "--branch", help="Git branch to use.", default=None)
def examples(name, force, branch, output):
    """
    Download examples

    Step 1. List examples

      $ ploomber examples


    Step 2. Download an example

      $ ploomber examples -n templates/ml-basic -o my-pipeline


    Need help? https://ploomber.io/community
    """
    click.echo("Loading examples...")

    from ploomber import cli as cli_module
    from ploomber.telemetry import telemetry

    try:
        cli_module.examples.main(name=name, force=force, branch=branch, output=output)
    except click.ClickException:
        raise
    except Exception as e:
        telemetry.log_api(
            "examples_error",
            metadata={"type": "runtime_error", "exception": str(e), "argv": sys.argv},
        )
        raise RuntimeError(
            "An error happened when executing the examples command. Check out "
            "the full error message for details. Downloading the examples "
            "again or upgrading Ploomber may fix the "
            "issue.\nDownload: ploomber examples -f\n"
            "Update: pip install ploomber -U\n"
            "Update [conda]: conda update ploomber -c conda-forge"
        ) from e


def _exit_with_error_message(msg):
    click.echo(msg, err=True)
    sys.exit(2)


def cmd_router():
    """CLI entry point"""
    cmd_name = None if len(sys.argv) < 2 else sys.argv[1]

    # These are parsing dynamic parameters and that's why we're isolating it.
    custom = ["build", "plot", "task", "report", "interact", "status", "nb"]

    # users may attempt to run execute/run, suggest to use build instead
    # users may make typos when running one of the commands
    # suggest correct spelling on obvious typos

    if cmd_name in custom:
        click.echo("Loading pipeline...")

        from ploomber import cli as cli_module

        # NOTE: we don't use the argument here, it is parsed by _main
        # pop the second element ('entry') to make the CLI behave as expected
        sys.argv.pop(1)
        # Add the current working directory, this is done automatically when
        # calling "python -m ploomber.build" but not here ("ploomber build")
        sys.path.insert(0, os.path.abspath("."))

        custom = {
            "build": cli_module.build.main,
            "plot": cli_module.plot.main,
            "task": cli_module.task.main,
            "report": cli_module.report.main,
            "interact": cli_module.interact.main,
            "status": cli_module.status.main,
            "nb": cli_module.nb.main,
        }

        fn = custom[cmd_name]
        fn()
    else:
        suggestion = _suggest_command(cmd_name, cli.commands.keys())

        # Set nested command keys based on command option
        # NOTE: we had cloud as nested command but we removed it, we keep this
        # here in case we add a new nested command
        nested_cmds = {}

        # Evaluate nested command and provide suggested command if applicable
        if cmd_name in nested_cmds:
            nested_cmd_name = None if len(sys.argv) < 3 else sys.argv[2]
            nested_suggestion = _suggest_command(nested_cmd_name, nested_cmds[cmd_name])
        else:
            nested_suggestion = None

        if suggestion:
            _exit_with_error_message(
                "Try 'ploomber --help' for help.\n\n"
                f"Error: {cmd_name!r} is not a valid command."
                f" Did you mean {suggestion!r}?"
            )
        elif nested_suggestion:
            cmds = f"{cmd_name} {nested_cmd_name}"
            suggestion = f"{cmd_name} {nested_suggestion}"
            suggested_help = f"ploomber {cmd_name} --help"

            _exit_with_error_message(
                f"Try {suggested_help!r} for help.\n\n"
                f"Error: {cmds!r} is not a valid command."
                f" Did you mean {suggestion!r}?"
            )
        else:
            cli()


# the commands below are handled by the router,
# those are a place holder to show up in ploomber --help
@cli.command()
def build():
    """Build pipeline"""
    pass  # pragma: no cover


@cli.command()
def status():
    """Show pipeline status"""
    pass  # pragma: no cover


@cli.command()
def plot():
    """Plot pipeline"""
    pass  # pragma: no cover


@cli.command()
def task():
    """Interact with specific tasks"""
    pass  # pragma: no cover


@cli.command()
def report():
    """Generate pipeline report"""
    pass  # pragma: no cover


@cli.command()
def interact():
    """Interact with your pipeline (REPL)"""
    pass  # pragma: no cover


@cli.command()
def nb():
    """Manage scripts and notebooks"""
    pass  # pragma: no cover
