import os
import re
import sys
from pathlib import Path

from jinja2 import Environment, PackageLoader
import click
from ploomber import __version__
from ploomber import cli as cli_module
from ploomber.spec.DAGSpec import DAGSpec

try:
    import importlib.resources as resources
except ImportError:
    import importlib_resources as resources


def _is_valid_name(package_name):
    match = re.match(r'^[\w-]+$', package_name) or False
    return match and not package_name[0].isnumeric()


@click.group()
@click.version_option(version=__version__)
def cli():
    """Ploomber command line interface.
    """
    pass


@cli.command()
def new():
    """Create a new project
    """
    _new()


@cli.command()
def add():
    """Create source files tasks registered in pipeline.yaml
    """
    _add()


class FileLoader:
    def __init__(self, directory, project_name=None):
        self.env = Environment(loader=PackageLoader(
            'ploomber', str(Path('resources', directory))),
                               variable_start_string='[[',
                               variable_end_string=']]',
                               block_start_string='[%',
                               block_end_string='%]')
        self.directory = directory
        self.project_name = project_name

    def get_template(self, name):
        return self.env.get_template(name)

    def copy(self, name):
        module = '.'.join(['ploomber', 'resources', self.directory])
        content = resources.read_text(module, name)
        Path(self.project_name, name).write_text(content)


def _add():
    spec, path = DAGSpec.auto_load(to_dag=False)
    env = FileLoader('ploomber_add')

    if path:
        click.echo('Found spec at {}'.format(path))

        for task in spec['tasks']:
            source = Path(task['source'])

            if not source.exists():
                # create parent folders if needed
                source.parent.mkdir(parents=True, exist_ok=True)

                if source.suffix in {'.py', '.sql'}:
                    click.echo('Adding {}...'.format(source))
                    template = env.get_template('task' + source.suffix)
                    content = template.render(**spec['meta'])
                    source.write_text(content)

                else:
                    click.echo('Error: This command does not support adding '
                               'tasks with extension "{}", valid ones are '
                               '.py and .sql. Skipping {}'.format(
                                   source.suffix, source))

    else:
        click.echo('Error: No pipeline.yaml spec found...')


def _new():
    click.echo('This utility will guide you through the process of creating '
               'a new project')

    valid_name = False

    while not valid_name:
        name = click.prompt(
            'Enter a name for your project (allowed: '
            'alphanumeric, underscores and hyphens)',
            type=str)
        valid_name = _is_valid_name(name)

        if not valid_name:
            click.echo('"%s" is not a valid project name, choose another.' %
                       name)

    env = FileLoader('ploomber_new', project_name=name)

    click.echo('Creating %s/' % name)
    Path(name).mkdir()

    db = click.confirm('Do you need to connect to a database?')

    if db:
        click.echo('Adding db.py...')
        env.copy('db.py')

    click.echo('Adding pipeline.yaml...')
    content = env.get_template('pipeline.yaml').render(db=db)
    Path(name, 'pipeline.yaml').write_text(content)

    conda = click.confirm('Do you you want to use conda to '
                          'manage virtual environments (recommended)?')

    if conda:
        # check if conda is installed...
        click.echo('Adding environment.yml...')
        content = env.get_template('environment.yml').render(name=name)
        Path(name, 'environment.yml').write_text(content)

    click.echo('Adding README.md...')
    content = env.get_template('README.md').render(name=name,
                                                   db=db,
                                                   conda=conda)
    path_to_readme = Path(name, 'README.md')
    path_to_readme.write_text(content)

    click.echo('Adding raw.py, clean.py and plot.py...')
    env.copy('raw.py')
    env.copy('clean.py')
    env.copy('plot.py')
    click.echo('Done!')

    Path(name, 'output').mkdir()
    click.echo('Check out {} to get started'.format(path_to_readme))


def cmd_router():
    cmd_name = sys.argv[1]

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
    pass


@cli.command()
def status():
    """Show pipeline status
    """
    pass


@cli.command()
def plot():
    """Plot pipeline
    """
    pass


@cli.command()
def task():
    """Interact with specific tasks
    """
    pass


@cli.command()
def report():
    """Make a pipeline report
    """
    pass


@cli.command()
def interact():
    """Start an interactive session (use the "dag" variable)
    """
    pass