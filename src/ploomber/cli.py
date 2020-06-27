import sys
from functools import partial
import shutil
from pathlib import Path

from ploomber import __version__
from ploomber.entry import entry as entry_module
from ploomber import SourceLoader
import click


def _copy(filename, loader):
    shutil.copy(str(loader[filename].path), filename)


@click.group()
@click.version_option(version=__version__)
def cli():
    """Ploomber command line interface
    """
    pass


@cli.command()
def new():
    _new()


def _new():
    """Create a base project
    """
    loader = SourceLoader(path=str(Path('resources', 'ploomber-new')),
                          module='ploomber')
    copy = partial(_copy, loader=loader)

    click.echo('This utility will guide you through the process of starting '
               'a new project')
    click.echo('Adding pipeline.yaml...')
    copy('pipeline.yaml')

    db = click.confirm('Do you need to connect to a database?')

    if db:
        click.echo('Adding db.py...')
        copy('db.py')

    conda = click.confirm('Do you you want to use conda to '
                          'manage virtual environments (recommended)?')

    if conda:
        # check if conda is installed...
        click.echo('Adding environment.yml...')
        copy('environment.yml')

    click.echo('Adding clean.py and features.py...')
    copy('clean.py')
    copy('plot.py')

    Path('output').mkdir()

    click.echo("""
    Done! Now create your environment with the following command:
      conda env create --file environment.yml

    Then activate it:
      conda activate my-ploomber-project

    And build:
      ploomber entry pipeline.yaml

    Or start an interactive session (once it starts, use the "dag" object):
      ipython -i -m ploomber.entry pipeline.yaml -- --action status
    """)


@cli.command()
@click.argument('entry_point')
def entry(entry_point):
    # NOTE: we don't use the argument here, it is parsed by _main
    # pop the second element ('entry') to make the CLI behave as expected
    sys.argv.pop(1)
    # Add the current working directory, this is done automatically when
    # calling "python -m ploomber.entry" but not here ("ploomber entry")
    sys.path.append('')
    entry_module._main()
