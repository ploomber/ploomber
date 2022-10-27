import time
from pathlib import Path
import os
from difflib import get_close_matches
import sys
import json as json_module

from ploomber_scaffold import scaffold as scaffold_project
import click

CLICK_VERSION = int(click.__version__[0])
# NOTE: package_name was introduced in version 8
VERSION_KWARGS = dict(
    package_name='ploomber') if CLICK_VERSION >= 8 else dict()


def _suggest_command(name: str, options):
    if not name or name in options:
        return None

    name = name.lower()

    mapping = {
        'run': 'build',
        'execute': 'build',
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
    '--conda/--pip',
    '-c/-p',
    is_flag=True,
    default=None,
    help='Use environment.yaml/requirements.txt for dependencies',
)
@click.option(
    '--package',
    '-P',
    is_flag=True,
    help='Use package template (creates setup.py)',
)
@click.option(
    '--empty',
    '-E',
    is_flag=True,
    help='Create a pipeline.yaml with no tasks',
)
@click.option(
    '--entry-point',
    '-e',
    default=None,
    help='Entry point to add tasks. Invalid if other flags present',
)
@click.argument('name', required=False)
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

    template = '-e/--entry-point is not compatible with {flag}'
    user_passed_name = name is not None

    if entry_point and name:
        err = '-e/--entry-point is not compatible with the "name" argument'
        telemetry.log_api("scaffold_error",
                          metadata={
                              'type': 'entry_and_name',
                              'exception': err,
                              'argv': sys.argv
                          })
        raise click.ClickException(err)

    if entry_point and conda:
        err = template.format(flag='--conda')
        telemetry.log_api("scaffold_error",
                          metadata={
                              'type': 'entry_and_conda_flag',
                              'exception': err,
                              'argv': sys.argv
                          })
        raise click.ClickException(err)

    if entry_point and package:
        err = template.format(flag='--package')
        telemetry.log_api("scaffold_error",
                          metadata={
                              'type': 'entry_and_package_flag',
                              'exception': err,
                              'argv': sys.argv
                          })
        raise click.ClickException(err)

    if entry_point and empty:
        err = template.format(flag='--empty')
        telemetry.log_api("scaffold_error",
                          metadata={
                              'type': 'entry_and_empty_flag',
                              'exception': err,
                              'argv': sys.argv
                          })
        raise click.ClickException(err)

    # try to load a dag by looking in default places
    if entry_point is None:
        loaded = _scaffold.load_dag()
    else:
        from ploomber.spec import DAGSpec

        try:
            loaded = (
                DAGSpec(entry_point, lazy_import='skip'),
                Path(entry_point).parent,
                Path(entry_point),
            )
        except Exception as e:
            telemetry.log_api("scaffold_error",
                              metadata={
                                  'type': 'dag_load_failed',
                                  'exception': str(e),
                                  'argv': sys.argv
                              })
            raise click.ClickException(e) from e

    if loaded:
        if user_passed_name:
            click.secho(
                "The 'name' positional argument is "
                "only valid for creating new projects, ignoring...",
                fg='yellow')

        # existing pipeline, add tasks
        spec, _, path_to_spec = loaded
        _scaffold.add(spec, path_to_spec)

        telemetry.log_api("ploomber_scaffold",
                          metadata={
                              'type': 'add_task',
                              'argv': sys.argv,
                              'dag': loaded,
                          })
    else:
        # no pipeline, create base project
        telemetry.log_api("ploomber_scaffold",
                          metadata={
                              'type': 'base_project',
                              'argv': sys.argv
                          })
        scaffold_project.cli(project_path=name,
                             conda=conda,
                             package=package,
                             empty=empty)


@cli.command()
@click.option('--use-lock/--no-use-lock',
              '-l/-L',
              default=None,
              help=('Use lock/regular files. If not present, uses any '
                    'of them, prioritizing lock files'))
@click.option('--create-env',
              '-e',
              is_flag=True,
              help=('Create a new environment, otherwise install in the '
                    'current environment'))
@click.option(
    '--use-venv',
    '-v',
    is_flag=True,
    help='Use Python\'s venv module (ignoring conda if installed)',
)
def install(use_lock, create_env, use_venv):
    """
    Install dependencies
    """
    from ploomber import cli as cli_module

    cli_module.install.main(use_lock=use_lock,
                            create_env=create_env,
                            use_venv=use_venv)


@cli.command()
@click.option('-n', '--name', help='Example to download', default=None)
@click.option('-f', '--force', help='Force examples download', is_flag=True)
@click.option('-o', '--output', help='Target directory', default=None)
@click.option('-b', '--branch', help='Git branch to use.', default=None)
def examples(name, force, branch, output):
    """
    Download examples

    Step 1. List examples

      $ ploomber examples


    Step 2. Download an example

      $ ploomber examples -n templates/ml-basic -o my-pipeline


    Need help? https://ploomber.io/community
    """
    click.echo('Loading examples...')

    from ploomber import cli as cli_module
    from ploomber.telemetry import telemetry

    try:
        cli_module.examples.main(name=name,
                                 force=force,
                                 branch=branch,
                                 output=output)
    except click.ClickException:
        raise
    except Exception as e:
        telemetry.log_api("examples_error",
                          metadata={
                              'type': 'runtime_error',
                              'exception': str(e),
                              'argv': sys.argv
                          })
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
    """CLI entry point
    """
    cmd_name = None if len(sys.argv) < 2 else sys.argv[1]

    # These are parsing dynamic parameters and that's why we're isolating it.
    custom = ['build', 'plot', 'task', 'report', 'interact', 'status', 'nb']

    # users may attempt to run execute/run, suggest to use build instead
    # users may make typos when running one of the commands
    # suggest correct spelling on obvious typos

    if cmd_name in custom:
        click.echo('Loading pipeline...')

        from ploomber import cli as cli_module

        # NOTE: we don't use the argument here, it is parsed by _main
        # pop the second element ('entry') to make the CLI behave as expected
        sys.argv.pop(1)
        # Add the current working directory, this is done automatically when
        # calling "python -m ploomber.build" but not here ("ploomber build")
        sys.path.insert(0, os.path.abspath('.'))

        custom = {
            'build': cli_module.build.main,
            'plot': cli_module.plot.main,
            'task': cli_module.task.main,
            'report': cli_module.report.main,
            'interact': cli_module.interact.main,
            'status': cli_module.status.main,
            'nb': cli_module.nb.main,
        }

        fn = custom[cmd_name]
        fn()
    else:
        suggestion = _suggest_command(cmd_name, cli.commands.keys())

        # Set nested command keys based on command option
        nested_cmds = {'cloud': cloud.commands.keys()}

        # Evaluate nested command and provide suggested command if applicable
        if cmd_name in nested_cmds:
            nested_cmd_name = None if len(sys.argv) < 3 else sys.argv[2]
            nested_suggestion = _suggest_command(nested_cmd_name,
                                                 nested_cmds[cmd_name])
        else:
            nested_suggestion = None

        if suggestion:
            _exit_with_error_message(
                "Try 'ploomber --help' for help.\n\n"
                f"Error: {cmd_name!r} is not a valid command."
                f" Did you mean {suggestion!r}?")
        elif nested_suggestion:
            cmds = f'{cmd_name} {nested_cmd_name}'
            suggestion = f'{cmd_name} {nested_suggestion}'
            suggested_help = f'ploomber {cmd_name} --help'

            _exit_with_error_message(f"Try {suggested_help!r} for help.\n\n"
                                     f"Error: {cmds!r} is not a valid command."
                                     f" Did you mean {suggestion!r}?")
        else:
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
    """Generate pipeline report
    """
    pass  # pragma: no cover


@cli.command()
def interact():
    """Interact with your pipeline (REPL)
    """
    pass  # pragma: no cover


@cli.command()
def nb():
    """Manage scripts and notebooks
    """
    pass  # pragma: no cover


@cli.group()
def cloud():
    """Manage Ploomber Cloud
    """
    pass  # pragma: no cover


@cloud.command()
@click.argument('user_key', required=True)
def set_key(user_key):
    """Set API key

    It validates it's a valid key. If the key is valid, it stores it in the
    user config.yaml file.
    """
    from ploomber import cli as cli_module
    cli_module.cloud.set_key(user_key)


@cloud.command()
def get_key():
    """Return API key

    It retrieves it from the user config.yaml file. Returns the key if it
    exists
    """
    from ploomber import cli as cli_module

    key = cli_module.cloud.get_key()

    if key:
        click.echo(f'This is your cloud API key: {key}')
    else:
        click.echo('No cloud API key was found.')


@cloud.command()
@click.argument('pipeline_id', default=None, required=False)
@click.option('-v', '--verbose', default=False, is_flag=True, required=False)
def get_pipelines(pipeline_id, verbose):
    """Get pipeline status

    Specify a pipeline_id to get it's state, when not specified, pulls all of
    the pipelines. You can pass "latest", instead of pipeline_id to get the
    latest. To get a detailed view pass the verbose flag.
    Returns a list of pipelines is succeeds, an Error string if fails.
    """
    from ploomber.table import Table
    from ploomber import cli as cli_module

    pipeline = cli_module.cloud.get_pipeline(pipeline_id, verbose)
    if isinstance(pipeline, list) and pipeline:
        pipeline = Table.from_dicts(pipeline, complete_keys=True)
    print(pipeline)


@cloud.command()
@click.argument('pipeline_id', required=True)
@click.argument('status', required=True)
@click.argument('log', default=None, required=False)
@click.argument('dag', default=None, required=False)
@click.argument('pipeline_name', default=None, required=False)
def write_pipeline(pipeline_id, status, log, pipeline_name, dag):
    """Write a pipeline

    Pipeline_id & status are required. You can also add logs and a pipeline
    name. Returns a string with pipeline id if succeeds, an Error string if
    fails.
    """
    from ploomber import cli as cli_module

    print(
        cli_module.cloud.write_pipeline(pipeline_id, status, log,
                                        pipeline_name, dag))


@cloud.command()
@click.argument('pipeline_id', required=True)
def delete_pipeline(pipeline_id):
    """Delete a pipeline

    pipeline_id is required. Returns a string with pipeline id if succeeds, an
    Error string if fails.
    """
    from ploomber import cli as cli_module
    print(cli_module.cloud.delete_pipeline(pipeline_id))


@cloud.command(name='build')
@click.option('-f',
              '--force',
              help='Force execution by ignoring status',
              is_flag=True)
@click.option('--json', is_flag=True)
def cloud_build(force, json):
    """Build pipeline in the cloud:

    $ ploomber cloud build

    Force execution regardless of product status:

    $ ploomber cloud build --force
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()
    runid = api.build(force,
                      github_number=None,
                      github_owner=None,
                      github_repo=None,
                      verbose=not json,
                      task=None)
    if json:
        click.echo(json_module.dumps(dict(runid=runid)))


@cloud.command(name='task')
@click.argument('task_name')
@click.option('-f',
              '--force',
              help='Force execution by ignoring status',
              is_flag=True)
@click.option('--json', is_flag=True)
def cloud_task(task_name, force, json):
    """Build a single pipeline task:

    $ ploomber cloud task {task-name}

    Force task execution regardless of status:

    $ ploomber cloud task {task-name} --force
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()
    runid = api.build(force,
                      github_number=None,
                      github_owner=None,
                      github_repo=None,
                      verbose=not json,
                      task=task_name)
    if json:
        click.echo(json_module.dumps(dict(runid=runid)))


@cloud.command(name="list")
@click.option('--json', is_flag=True)
def cloud_list(json):
    """List executions:

    $ ploomber cloud list
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()
    api.runs(json=json)


@cloud.command(name="status")
@click.argument('run_id')
@click.option('--watch', '-w', is_flag=True)
@click.option('--json', is_flag=True)
def cloud_status(run_id, watch, json):
    """Get task's execution status:

    $ ploomber cloud status {some-id}

    Get task's status of latest execution:

    $ ploomber cloud status @latest
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()

    if watch:
        idle = 5
        timeout = 10 * 60
        cumsum = 0

        while cumsum < timeout:
            click.clear()
            out = api.run_detail_print(run_id)

            status = set([t['status'] for t in out['tasks']])

            if out['run'] != 'created' and (status == {'finished'}
                                            or 'aborted' in status or 'failed'
                                            in status) or cumsum >= timeout:
                break

            time.sleep(idle)
            cumsum += idle
    else:
        api.run_detail_print(run_id, json=json)


@cloud.command(name="products")
@click.option('-d', '--delete', default=None)
@click.option('--json', is_flag=True)
def cloud_products(delete, json):
    """Manage products (outputs) in cloud workspace

    List products in your workspace:

    $ ploomber cloud products

    Delete products in your workspace:

    $ ploomber cloud products --delete {pattern}

    Delete all .csv files in your workspace:

    $ ploomber cloud products --delete '*.csv'
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()

    if delete:
        api.delete_products(delete)
    else:
        api.products_list(json=json)


@cloud.command(name="download")
@click.argument('pattern')
def cloud_download(pattern):
    """Download products from cloud workspace:

    $ ploomber cloud download {pattern}

    Download all .csv files:

    $ ploomber cloud download '*.csv'
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()
    api.products_download(pattern)


@cloud.command(name="logs")
@click.argument('run_id')
@click.option('--image', '-i', is_flag=True)
@click.option('--watch', '-w', is_flag=True)
@click.option('--task', '-t', default=None)
def cloud_logs(run_id, image, watch, task):
    """Get logs for all tasks in a cloud run

    Get task logs:

    $ ploomber cloud logs {some-runid}

    Get logs for a given task name:

    $ ploomber cloud logs {some-runid} {some-task-name}

    Get Docker image building logs:

    $ ploomber cloud logs {some-runid} --image

    Get task logs for the latest run:

    $ ploomber cloud logs @latest

    Watch logs:

    $ ploomber cloud logs @latest --watch
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()

    if image:
        if watch:
            idle = 2
            timeout = 10 * 60
            cumsum = 0

            while cumsum < timeout:
                click.clear()
                time.sleep(idle)
                done = api.run_logs_image(run_id, tail=20)
                cumsum += idle

                if done:
                    break
        else:
            api.run_logs_image(run_id)
    else:
        api.run_logs(run_id, task)


@cloud.command(name="abort")
@click.argument('run_id')
def cloud_abort(run_id):
    """Abort a cloud execution:

    $ ploomber cloud abort {some-id}

    Abort latest run:

    $ ploomber cloud abort @latest
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()
    api.run_abort(run_id)


@cloud.command(name="data")
@click.option('-u', '--upload', default=None)
@click.option('-p', '--prefix', default=None)
@click.option('-n', '--name', default=None)
@click.option('-d', '--delete', default=None)
def cloud_data(upload, delete, prefix, name):
    """Manage input data workspace

    List data files:

    $ ploomber cloud data

    Upload data:

    $ ploomber cloud data --upload dataset.parquet

    Upload to a specific directory:

    $ ploomber cloud data --upload dataset.parquet --prefix my-dataset

    Upload with a specific name:

    $ ploomber cloud data --upload dataset.parquet --name data.parquet

    Delete data (deletes all the objects matching the pattern):

    $ ploomber cloud data --delete '*.parquet'

    In your notebook (file will be downloaded to data/dataset.parquet):

    >>> from ploomber.cloud import download_data

    >>> download_data('dataset.parquet') # doctest: +SKIP
    """
    from ploomber.cloud.api import PloomberCloudAPI
    api = PloomberCloudAPI()

    # one arg max

    if upload:
        api.upload_data(upload, prefix=prefix, key=name)
    elif delete:
        if prefix:
            click.echo(
                'prefix has no effect when using -d/--delete. Ignoring...')

        if name:
            click.echo(
                'name has no effect when using -d/--delete. Ignoring...')

        api.delete_data(delete)
    else:
        if prefix:
            click.echo('prefix has no effect when listing files. Ignoring...')

        if name:
            click.echo('name has no effect when listing files. Ignoring...')

        api.data_list()


@cloud.command(name="nb")
@click.argument('path_to_notebook')
@click.option('--json', is_flag=True)
def cloud_notebook(path_to_notebook, json):
    """Run a notebook

    $ ploomber cloud nb path/to/notebook.ipynb
    """
    from ploomber.cloud.api import PloomberCloudAPI
    from ploomber.cloud import io

    # TODO: add unit test
    path_to_notebook = io.download_notebook_if_needed(path_to_notebook)

    if not Path(path_to_notebook).is_file():
        raise click.ClickException(f"{path_to_notebook} does not exist")

    api = PloomberCloudAPI()
    response_upload = api.upload_data(path_to_notebook,
                                      prefix='notebooks',
                                      key=Path(path_to_notebook).name,
                                      version=True,
                                      verbose=not json)
    key = response_upload['key']

    if not json:
        click.echo(f'Triggering execution of {key}...')

    response_execute = api.notebooks_execute(key, json_=json)

    if json:
        response = {
            'response_upload': response_upload,
            'response_execute': response_execute
        }

        click.echo(json_module.dumps(response))
    else:
        runid = response_execute['runid']
        click.secho(
            "Done. Monitor Docker build process with:\n  "
            f"$ ploomber cloud logs {runid} --image --watch",
            fg='green')
