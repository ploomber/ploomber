import os
import platform
import shutil
import time
import subprocess
from pathlib import Path
import sys
import click

import jupytext
import nbformat

from ploomber.cli.examples import _ExamplesManager
from ploomber.cli.nb import _inject_cell, _format
from ploomber.spec import DAGSpec
from ploomber.constants import TaskStatus
from ploomber.cli.io import command_endpoint
from ploomber.telemetry import telemetry

WINDOWS = platform.system() == 'Windows'


def _confirm(message):
    message = 'continue' if message is None else message
    click.prompt(f'Press enter {message}', default='', show_default=False)


def _echo(msg, confirm=True, prefix='\n'):
    click.secho(prefix + msg, fg='green')

    if confirm:
        _confirm(message=None if confirm is True else confirm)


def _make_open_callable(binary_name):
    def _callable(path):
        subprocess.run([binary_name, path])

    return _callable


def _get_open_callable():
    if WINDOWS:
        return os.startfile

    open_binary = None

    if shutil.which('open'):
        open_binary = 'open'

    if shutil.which('xdg-open'):
        open_binary = 'open'

    if open_binary:
        return _make_open_callable(open_binary)


def _try_open(path):
    callable = _get_open_callable()

    if callable:
        click.echo(f'Opening {path}...')
        time.sleep(1)
        # TODO: test we're calling it with the right arg. if we call dir/file
        # on windows it wont work
        callable(str(Path(path)))


def _try_open_with_message(path):
    click.echo(f'Done! Open: {path}')
    _try_open(path)


def _load_dag(directory):
    return DAGSpec(f'{directory}/pipeline.yaml').to_dag().render(
        show_progress=False)


def _modified_task(directory):
    dag = _load_dag(directory)
    return dag['fit']._exec_status == TaskStatus.WaitingExecution


def _cmd(cmd):
    # maybe add a opening in 3,2,1? otherwise it's surprising..
    # or maybe in the "press enter to continue", switch for
    # press enter to open but default to "to continue" if we cannot
    # open it
    click.secho(f'\nRunning:\n    $ {cmd}\n', fg='bright_blue')


@command_endpoint
@telemetry.log_call('onboard', payload=True)
def main(payload):
    """Runs the onboarding tutorial
    """
    payload['step'] = '0-start'

    click.clear()
    _echo(
        'Ploomber allows you to write modular '
        'data pipelines. Let me show you how!',
        prefix='')

    # TODO: ask for email

    EDITOR = click.prompt('What editor do you use?',
                          type=click.Choice(['jupyter', 'other']),
                          default='other')
    FILENAME = 'fit.py' if EDITOR == 'other' else 'fit.ipynb'
    DIRECTORY = ('example'
                 if not Path('example').exists() else 'ploomber-example')
    PATH = Path(DIRECTORY, FILENAME)
    CAN_OPEN = _get_open_callable()
    payload['step'] = '1-editor'

    # download example
    click.echo("Let's download an example...")
    manager = _ExamplesManager(verbose=False)
    # TODO: if example exists, try a different name
    manager.download('templates/ml-basic', DIRECTORY)
    _cmd(f'ploomber examples -n templates/ml-basic -o {DIRECTORY}')
    click.echo(f"Done! Example downloaded at {DIRECTORY}/")
    payload['step'] = '2-example-download'

    # install dependencies?
    _echo("Let's install the dependencies!", confirm='install dependencies')
    bin = sys.executable
    pip_install = ['pip', 'install', '-r', f'{DIRECTORY}/requirements.txt']
    subprocess.run([
        bin,
        '-m',
    ] + pip_install, check=True)
    payload['step'] = '3-install'

    # inject cell
    # maybe let's convert ml-basic to script-only pipeline?
    sys.path.insert(0, DIRECTORY)

    dag = _load_dag(DIRECTORY)

    if EDITOR == 'jupyter':
        _format(fmt='ipynb',
                entry_point=f'{DIRECTORY}/pipeline.yaml',
                dag=dag,
                verbose=False)
        dag = _load_dag(DIRECTORY)

    here = Path().resolve()
    source = Path(dag['fit'].source.loc).relative_to(here)
    product = Path(dag['fit'].product['nb']).relative_to(here)
    _inject_cell(dag)

    # if they use jupyter, maybe convert to ipynb? there's a chance the
    # jupyter plugin isn't correctly configured
    click.clear()
    click.echo('Done installing dependencies!')
    _echo("Let's plot the pipeline!",
          confirm='to plot and open the file' if CAN_OPEN else 'to plot')
    _cmd('ploomber plot')
    dag.plot(f'{DIRECTORY}/pipeline.html', backend='d3')
    _try_open_with_message(f'{DIRECTORY}/pipeline.html')
    payload['step'] = '4-plot'

    # explain pipeline.yaml
    _echo(
        'In Ploomber, you specify your pipeline tasks in '
        'a YAML file, take a look: '
        f'{DIRECTORY}/pipeline.yaml',
        confirm='to open the file' if CAN_OPEN else 'continue')
    _try_open(f'{DIRECTORY}/pipeline.yaml')
    payload['step'] = '5-pipeline-yaml'

    # show ploomber plot

    _echo("You can execute the pipeline with one command"
          ", let's do it",
          confirm='to run the pipeline')
    _cmd('ploomber build')

    # run ploomber build (show parallel tasks)
    report = dag.build()
    click.echo(report)

    # TODO: change for "you just trained X models in parallel, nice!"
    click.echo('Done running the pipeline!')
    payload['step'] = '6-build'

    trials = 0

    while not _modified_task(DIRECTORY):

        if trials == 0:
            click.secho(
                '\nPloomber keeps track of source code '
                f'changes so you can iterate faster, please edit {source}',
                fg='green')

            if EDITOR == 'other':
                _try_open(source)

            _confirm(message=f'once you edited {source}')

        else:
            _echo(f'Looks like {source} did not change. Please edit it.',
                  confirm=f'once you edited {source}')

        if trials == 1:
            _echo('Let me modify the file for you.',
                  confirm=f'to automatically edit {DIRECTORY}/{FILENAME}')

            nb = jupytext.reads(source.read_text())
            version = nbformat.versions[nb.nbformat]
            cell = version.new_code_cell('print("adding one cell!")')
            nb.cells.append(cell)
            jupytext.write(nb, PATH)
            click.echo(f'Done editing {source}')

        trials += 1

    payload['step'] = '6-task-edit'

    # run ploomber build (incremental builds)
    click.echo('Running the pipeline...')
    dag = _load_dag(DIRECTORY)
    dag.render(force=True)
    report = dag.build()
    click.echo(report)
    payload['step'] = '7-build-incremental'

    # show html reports
    click.echo(f'\nDone! Note that only {source} was executed!')
    _echo(
        'Ploomber automatically generates reports from each '
        f'script/notebook, check it out: {product}',
        confirm='to open the report' if CAN_OPEN else 'continue')
    _try_open(product)
    payload['step'] = '8-report'

    # link to the cloud
    # TODO: automatically run it in the cloud (maybe with a key that has
    # some limits)
    click.clear()
    _echo(
        "Sometimes a laptop is not enough. To easily run "
        "your pipeline on a cluster, use Ploomber Cloud (It's FREE!)",
        confirm=False,
        prefix='')
    click.secho("https://ploomber.io/s/cloud", fg='bright_blue')
    payload['step'] = '9-cloud'

    _confirm(message=None)

    _echo("That's it! Please join our community to stay in touch!",
          confirm=False)
    click.secho("https://ploomber.io/community", fg='bright_blue')

    payload['step'] = '10-community'


if __name__ == '__main__':
    main()
