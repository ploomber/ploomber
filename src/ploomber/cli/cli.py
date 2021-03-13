import os
import sys
from pathlib import Path

import click
from ploomber import __version__
from ploomber import cli as cli_module
from ploomber import scaffold as _scaffold
from ploomber_scaffold import scaffold as scaffold_project


@click.group()
@click.version_option(version=__version__)
def cli():
    """Ploomber command line interface.
    """
    pass  # pragma: no cover


@cli.command()
def scaffold():
    """Create new projects and add template tasks
    """
    if Path('pipeline.yaml').exists():
        _scaffold.add()
    else:
        scaffold_project.cli(project_path=None)


@cli.command()
@click.option('-n', '--name', help='Example to use', default=None)
@click.option('-f', '--force', help='Force examples download', is_flag=True)
@click.option('-b',
              '--branch',
              help='Git branch to use. Defaults to master',
              default='master')
def examples(name, force, branch):
    """Get sample projects. Run "ploomber examples" to list them
    """
    cli_module.examples.main(name=name, force=force, branch=branch)


def cmd_router():
    cmd_name = None if len(sys.argv) < 2 else sys.argv[1]

    custom = {
        'build': cli_module.build.main,
        'plot': cli_module.plot.main,
        'task': cli_module.task.main,
        'report': cli_module.report.main,
        'interact': cli_module.interact.main,
        'status': cli_module.status.main,
    }

    if cmd_name in custom:
        # NOTE: we don't use the argument here, it is parsed by _main
        # pop the second element ('entry') to make the CLI behave as expected
        sys.argv.pop(1)
        # Add the current working directory, this is done automatically when
        # calling "python -m ploomber.build" but not here ("ploomber build")
        sys.path.insert(0, os.path.abspath('.'))
        fn = custom[cmd_name]
        fn()
    else:
        cli()


# the commands below are handled by the router, thy are just here so they
# show up when doing ploomber --help
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
