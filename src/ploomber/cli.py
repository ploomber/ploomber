import sys
from functools import partial
from pathlib import Path

from jinja2 import Environment, PackageLoader
import click
from ploomber import __version__
from ploomber.entry import entry as entry_module
from ploomber.spec.DAGSpec import DAGSpec


def _copy(filename, env):
    content = env.get_template(filename).render()
    Path(filename).write_text(content)


@click.group()
@click.version_option(version=__version__)
def cli():
    """Ploomber command line interface.

    To start an nteractive session (use "dag" variable when it starts):

    ipython -i -m ploomber.entry pipeline.yaml -- --action status
    """
    pass


@cli.command()
def new():
    """Create a new project
    """
    _new()


@cli.command()
def add():
    """Add tasks declared in pipeline.yaml whose source code files do not exist
    """
    _add()


def _add():
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

        for task in spec['tasks']:
            source = Path(task['source'])

            if not source.exists():
                # create parent folders if needed
                source.parent.mkdir(parents=True, exist_ok=True)

                if source.suffix in {'.py', '.sql'}:
                    click.echo('Adding {}...'.format(source))
                    template = env.get_template('task'+source.suffix)
                    content = template.render(**spec['meta'])
                    source.write_text(content)

                else:
                    click.echo('Error: This command does not support adding '
                               'tasks with extension "{}", valid ones are '
                               '.py and .sql. Skipping {}'
                               .format(source.suffix, source))

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

    Then activate it (you can change the name by editing environment.yml):
      conda activate my-project
    """ + out_message

    click.echo(out_message)


@cli.command()
@click.argument('entry_point')
def entry(entry_point):
    """Call an entry point (pipeline.yaml or dotted path to factory)
    """
    # NOTE: we don't use the argument here, it is parsed by _main
    # pop the second element ('entry') to make the CLI behave as expected
    sys.argv.pop(1)
    # Add the current working directory, this is done automatically when
    # calling "python -m ploomber.entry" but not here ("ploomber entry")
    sys.path.append('')
    entry_module._main()
