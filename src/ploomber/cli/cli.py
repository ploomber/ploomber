from pathlib import Path
import os
import sys

import click

from ploomber.spec import DAGSpec
from ploomber import __version__
from ploomber import cli as cli_module
from ploomber import scaffold as _scaffold
from ploomber_scaffold import scaffold as scaffold_project
from ploomber.telemetry import telemetry


@click.group()
@click.version_option(version=__version__)
def cli():
    """Ploomber command line interface.
    """
    pass  # pragma: no cover


@cli.command()
@click.option(
    '--conda',
    is_flag=True,
    help='Use conda (environemnt.yml)',
)
@click.option(
    '--package',
    is_flag=True,
    help='Use package template (setup.py)',
)
@click.option(
    '--empty',
    is_flag=True,
    help='Create a sample pipeline.yaml with no tasks',
)
@click.option(
    '--entry-point',
    '-e',
    default=None,
    help='Entry point to add tasks. Invalid if other flags present',
)
def scaffold(conda, package, entry_point, empty):
    """Create new projects (if no pipeline.yaml exists) or add missings tasks
    """
    template = '-e/--entry-point is not compatible with the {flag} flag'

    if entry_point and conda:
        raise click.ClickException(template.format(flag='--conda'))

    if entry_point and package:
        raise click.ClickException(template.format(flag='--package'))

    if entry_point and empty:
        raise click.ClickException(template.format(flag='--empty'))

    # try to load a dag by looking in default places
    if entry_point is None:
        loaded = _scaffold.load_dag()
    else:
        try:
            loaded = (
                DAGSpec(entry_point, lazy_import='skip'),
                Path(entry_point).parent,
                Path(entry_point),
            )
        except Exception as e:
            raise click.ClickException(e) from e

    if loaded:
        # existing pipeline, add tasks
        spec, _, path_to_spec = loaded
        _scaffold.add(spec, path_to_spec)
    else:
        # no pipeline, create base project
        scaffold_project.cli(project_path=None,
                             conda=conda,
                             package=package,
                             empty=empty)


@cli.command()
@click.option('-l',
              '--use-lock',
              help='Use lock files',
              default=False,
              is_flag=True)
def install(use_lock):
    """Install dependencies and package
    """
    cli_module.install.main(use_lock=use_lock)


@cli.command()
@click.option('-n', '--name', help='Example to download', default=None)
@click.option('-f', '--force', help='Force examples download', is_flag=True)
@click.option('-o', '--output', help='Target directory', default=None)
@click.option('-b', '--branch', help='Git branch to use.', default=None)
def examples(name, force, branch, output):
    """Get sample projects. Run "ploomber examples" to list them
    """
    try:
        cli_module.examples.main(name=name,
                                 force=force,
                                 branch=branch,
                                 output=output)
    except click.ClickException:
        raise
    except Exception as e:
        raise RuntimeError(
            'An error happened when executing the examples command. Check out '
            'the full error message for details. Downloading the examples '
            'again or upgrading Ploomber may fix the '
            'issue.\nDownload: ploomber examples -f\n'
            'Update: pip install ploomber -U\n'
            'Update [conda]: conda update ploomber -c conda-forge') from e


def _exit_with_error_message(msg):
    click.echo(msg, err=True)
    sys.exit(2)


def cmd_router():
    cmd_name = None if len(sys.argv) < 2 else sys.argv[1]

    custom = {
        'build': cli_module.build.main,
        'plot': cli_module.plot.main,
        'task': cli_module.task.main,
        'report': cli_module.report.main,
        'interact': cli_module.interact.main,
        'status': cli_module.status.main,
        'nb': cli_module.nb.main,
    }

    # users may attempt to run execute/run, suggest to use build instead
    alias = {'execute': 'build', 'run': 'build'}

    if cmd_name in custom:
        # NOTE: we don't use the argument here, it is parsed by _main
        # pop the second element ('entry') to make the CLI behave as expected
        sys.argv.pop(1)
        # Add the current working directory, this is done automatically when
        # calling "python -m ploomber.build" but not here ("ploomber build")
        sys.path.insert(0, os.path.abspath('.'))
        fn = custom[cmd_name]
        fn()
    elif cmd_name in alias:
        suggestion = alias[cmd_name]
        _exit_with_error_message("Try 'ploomber --help' for help.\n\n"
                                 f"Error: {cmd_name!r} is not a valid command."
                                 f" Did you mean {suggestion!r}?")
    else:
        telemetry.log_api("unsupported-api-call", metadata={'argv': sys.argv})
        cli()


# the commands below are handled by the router,
# those are a place holder to show up in ploomber --help
@cli.command()
def build():
    """Build pipeline
    """
    pass  # pragma: no cover


@cli.command()
def status():
    """Show pipeline status
    """
    pass  # pragma: no cover


@cli.command()
def plot():
    """Plot pipeline
    """
    pass  # pragma: no cover


@cli.command()
def task():
    """Interact with specific tasks
    """
    pass  # pragma: no cover


@cli.command()
def report():
    """Make a pipeline report
    """
    pass  # pragma: no cover


@cli.command()
def interact():
    """Start an interactive session (use the "dag" variable)
    """
    pass  # pragma: no cover


@cli.command()
def nb():
    """Manage scripts and notebooks
    """
    pass  # pragma: no cover
