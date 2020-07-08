import sys
from functools import partial
from pathlib import Path

from jinja2 import Environment, PackageLoader
import click
import yaml
from ploomber import __version__
from ploomber.entry import entry as entry_module
from ploomber.spec.DAGSpec import DAGSpec


def _copy(filename, env):
    content = env.get_template(filename).render()
    Path(filename).write_text(content)


@click.group()
@click.version_option(version=__version__)
def cli():
    """Ploomber command line interface
    """
    pass


@cli.command()
def new():
    """Create a base project
    """
    _new()


@cli.command()
@click.argument('name')
def add(name):
    """Add a new task
    """
    _add(name)


def _add(name):
    name = Path(name)

    spec, path = DAGSpec.auto_load(to_dag=False)

    if path:
        click.echo('Found spec at {}'.format(path))

        env = Environment(
            loader=PackageLoader('ploomber', 'resources/ploomber-add'),
            variable_start_string='[[',
            variable_end_string=']]',
            block_start_string='[%',
            block_end_string='%]'
        )

        if name.exists():
            click.echo('Error: File "{}" already exists, delete it first if '
                       'you want to replace it.'.format(name))
        else:
            if name.suffix in {'.py', '.sql'}:
                click.echo('Adding {}...'.format(name))
                template = env.get_template('task'+name.suffix)
                content = template.render(**spec['meta'])
                name.write_text(content)

                template_task = env.get_template('task.yaml')
                content_task = template_task.render(source=str(name),
                                                    **spec['meta'])

                click.echo('Done!\nAdd the following entry to your '
                           'pipeline.yaml in the tasks section:\n\n{}'
                           .format(content_task))

            else:
                click.echo('Error: This command does not support adding tasks '
                           'with extension "{}", valid ones are .py and .sql'
                           .format(name.suffix))

    else:
        click.echo('Error: No pipeline.yaml spec found...')


def _new():
    env = Environment(
            loader=PackageLoader('ploomber', 'resources/ploomber-new'),
            variable_start_string='[[',
            variable_end_string=']]',
            block_start_string='[%',
            block_end_string='%]'
        )
    copy = partial(_copy, env=env)

    click.echo('This utility will guide you through the process of starting '
               'a new project')
    db = click.confirm('Do you need to connect to a database?')

    if db:
        click.echo('Adding db.py...')
        copy('db.py')

    click.echo('Adding pipeline.yaml...')
    content = env.get_template('pipeline.yaml').render(db=db)
    Path('pipeline.yaml').write_text(content)

    conda = click.confirm('Do you you want to use conda to '
                          'manage virtual environments (recommended)?')

    if conda:
        # check if conda is installed...
        click.echo('Adding environment.yml...')
        copy('environment.yml')

    click.echo('Adding clean.py and features.py...')
    copy('clean.py')
    copy('plot.py')
    click.echo('Done!')

    Path('output').mkdir()

    out_message = """
    To build the pipeline:
      ploomber entry pipeline.yaml

    Start an interactive session (once it starts, use the "dag" object):
      ipython -i -m ploomber.entry pipeline.yaml -- --action status
    """

    if conda:
        out_message = """
    Now create your environment with the following command:
      conda env create --file environment.yml

    Then activate it:
      conda activate my-ploomber-project
    """ + out_message

    click.echo(out_message)


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
