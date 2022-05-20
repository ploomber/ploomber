import argparse
import json
import shutil
from pathlib import Path
import stat

import click

from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import command_endpoint
from ploomber.telemetry import telemetry
from ploomber.sources.notebooksource import recursive_update
from ploomber.exceptions import BaseException


def _format(fmt, entry_point, dag, verbose=True):
    return [
        str(p) for p in _call_in_source(dag,
                                        'format',
                                        'Formatted notebooks',
                                        dict(fmt=fmt, entry_point=entry_point),
                                        verbose=verbose) if p is not None
    ]


def _inject_cell(dag):
    _call_in_source(dag,
                    'save_injected_cell',
                    'Injected cell',
                    dict(),
                    verbose=False)


def _call_in_source(dag, method_name, message, kwargs=None, verbose=True):
    """
    Execute method on each task.source in dag, passing kwargs
    """
    kwargs = kwargs or {}
    files = []
    results = []

    for task in dag.values():
        try:
            method = getattr(task.source, method_name)
        except AttributeError:
            pass
        else:
            results.append(method(**kwargs))
            files.append(str(task.source._path))

    files_ = '\n'.join((f'    {f}' for f in files))

    if verbose:
        click.echo(f'{message}:\n{files_}')

    return results


def _install_hook(path_to_hook, content, entry_point):
    """
    Install a git hook script at the given path
    """
    if path_to_hook.exists():
        raise RuntimeError(
            'hook already exists '
            f'at {path_to_hook}. Run: "ploomber nb -u" to uninstall the '
            'existing hook and try again')

    path_to_hook.write_text(content.format(entry_point=entry_point))
    # make the file executable
    path_to_hook.chmod(path_to_hook.stat().st_mode | stat.S_IEXEC)


def _delete_hook(path):
    """Delete a git hook at the given path
    """
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            # in the remote case that it's a directory
            shutil.rmtree(path)

    click.echo(f'Deleted hook located at {path}')


pre_commit_hook = """
# !/usr/bin/env bash
# Automatically generated pre-commit hook to remove the injected cell in
# scripts and notebook tasks

# remove injected cells
ploomber nb --entry-point {entry_point} --remove

# re-add files
git add $(git diff --name-only --cached)
"""

post_commit_hook = """
# !/usr/bin/env bash
# Automatically generated post-commit hook to add the injected cell in
# scripts and notebook tasks

# inject cells
ploomber nb --entry-point {entry_point} --inject
"""

# taken from https://github.com/mwouts/jupytext/blob/main/README.md#install
_jupyterlab_default_settings_overrides = """
{
  "@jupyterlab/docmanager-extension:plugin": {
    "defaultViewers": {
      "markdown": "Jupytext Notebook",
      "myst": "Jupytext Notebook",
      "r-markdown": "Jupytext Notebook",
      "quarto": "Jupytext Notebook",
      "julia": "Jupytext Notebook",
      "python": "Jupytext Notebook",
      "r": "Jupytext Notebook"
    }
  }
}
"""


def _py_with_single_click_enable():
    """
    Writes ~/.jupyterlab/labconfig/default_setting_overrides.json to enable
    opening .py files as notebooks with a single click. If the secion already
    exists, it overrides its value
    """
    parent = Path('~/.jupyter', 'labconfig').expanduser()
    path = parent / 'default_setting_overrides.json'

    if path.exists():
        target = json.loads(path.read_text())
    else:
        target = {}

    recursive_update(target,
                     json.loads(_jupyterlab_default_settings_overrides))

    click.echo(f'Overriding JupyterLab defaults at: {str(path)}')
    parent.mkdir(exist_ok=True, parents=True)
    path.write_text(json.dumps(target))
    click.secho(
        'Done. You can now open .py and other formats in JupyterLab '
        'with a single click. You may need to reload JupyterLab',
        fg='green')


def _py_with_single_click_disable():
    """
    Opens ~/.jupyterlab/labconfig/default_setting_overrides.json and deletes
    the value in
    ['@jupyterlab/docmanager-extension:plugin'][''defaultViewers'], if any
    """
    parent = Path('~/.jupyter', 'labconfig')
    target = (parent / 'default_setting_overrides.json').expanduser()

    if target.exists():
        content = json.loads(target.read_text())
        key1 = '@jupyterlab/docmanager-extension:plugin'
        key2 = 'defaultViewers'

        if content.get(key1, {}).get(key2):
            del content[key1][key2]

            if key1 in content and not content.get(key1):
                del content[key1]

            Path(target).write_text(json.dumps(content))

    click.secho(
        'Done. Disabled opening .py files and other formats in JupyterLab '
        'with a single click. You may need to reload JupyterLab',
        fg='yellow')


_description = """Manage scripts and notebooks


Inject cell in all scripts and notebooks:

$ ploomber nb -i

Enable one-click to open .py as notebooks in JupyterLab:

$ ploomber nb -S

Re-format .ipynb notebooks as .py files with the percent format:

$ ploomber nb -f py:percent

Re-format .py files as .ipynb notebooks:

$ ploomber nb -f ipynb
"""


# TODO: --log, --log-file should not appear as options
@command_endpoint
@telemetry.log_call('nb')
def main():
    parser = CustomParser(
        description=_description,
        prog='ploomber nb',
        # required for the --help text to keep line breaks
        formatter_class=argparse.RawTextHelpFormatter)

    with parser:
        # The next options do not require a valid entry point

        # opening .py files as notebooks in JupyterLab with a single click
        single_click = parser.add_mutually_exclusive_group()
        single_click.add_argument(
            '--single-click',
            '-S',
            action='store_true',
            help=('Override JupyterLab defaults to open '
                  'scripts as notebook with a single click'))
        single_click.add_argument(
            '--single-click-disable',
            '-d',
            action='store_true',
            help=('Disables opening scripts as notebook with a single '
                  'click in JupyterLab'))

        # install/uninstall hook
        hook = parser.add_mutually_exclusive_group()
        hook.add_argument('--install-hook',
                          '-I',
                          action='store_true',
                          help='Install git pre-commit hook')
        hook.add_argument('--uninstall-hook',
                          '-u',
                          action='store_true',
                          help='Uninstall git pre-commit hook')

        # The next options require a valid entry point

        # inject/remove cell
        cell = parser.add_mutually_exclusive_group()
        cell.add_argument('--inject',
                          '-i',
                          action='store_true',
                          help='Inject cell to all script/notebook tasks')
        cell.add_argument(
            '--remove',
            '-r',
            action='store_true',
            help='Remove injected cell in all script/notebook tasks')

        # re-format
        parser.add_argument('--format',
                            '-f',
                            help='Re-format script/notebook tasks '
                            '(values: "py:percent" and "ipynb")')

        # pair scripts and nbs
        parser.add_argument('--pair',
                            '-p',
                            help='Pair scripts with ipynb files')

        # sync scripts and nbs
        parser.add_argument('--sync',
                            '-s',
                            action='store_true',
                            help='Sync scripts with ipynb files')

    loading_error = None

    # commands that need an entry point to work
    needs_entry_point = {'format', 'inject', 'remove', 'sync', 'pair'}

    args_ = parser.parse_args()

    if any(getattr(args_, arg) for arg in needs_entry_point):
        try:
            dag, args = parser.load_from_entry_point_arg()
        except Exception as e:
            loading_error = e
        else:
            dag.render(show_progress=False)

        if loading_error:
            raise BaseException('Could not run nb command: the DAG '
                                'failed to load') from loading_error
    else:
        dag = None
        args = args_

    # options that do not need a DAG

    if args.single_click:
        _py_with_single_click_enable()

    if args.single_click_disable:
        _py_with_single_click_disable()

    if args.install_hook:
        if not Path('.git').is_dir():
            raise NotADirectoryError(
                'Expected a .git/ directory in the current working '
                'directory. Run this from the repository root directory.')

        parent = Path('.git', 'hooks')
        parent.mkdir(exist_ok=True)

        # pre-commit: remove injected cells
        _install_hook(parent / 'pre-commit', pre_commit_hook, args.entry_point)
        click.echo('Successfully installed pre-commit git hook')

        # post-commit: inject cells
        _install_hook(parent / 'post-commit', post_commit_hook,
                      args.entry_point)
        click.echo('Successfully installed post-commit git hook')

    if args.uninstall_hook:
        _delete_hook(Path('.git', 'hooks', 'pre-commit'))
        _delete_hook(Path('.git', 'hooks', 'post-commit'))

    # options that need a valid DAG

    if args.format:
        new_paths = _format(fmt=args.format,
                            entry_point=args.entry_point,
                            dag=dag)

        if len(new_paths):
            click.echo('Extension changed for the following '
                       f'tasks: {", ".join(new_paths)}.')

    if args.inject:
        _inject_cell(dag)
        click.secho(
            'Finished cell injection. Re-run this command if your '
            'pipeline.yaml changes.',
            fg='green')

    if args.remove:
        _call_in_source(
            dag,
            'remove_injected_cell',
            'Removed injected cell',
            dict(),
        )

    if args.sync:
        # maybe its more efficient to pass all notebook paths at once?
        _call_in_source(dag, 'sync', 'Synced notebooks')

    # can pair give trouble if we're reformatting?
    if args.pair:
        _call_in_source(
            dag,
            'pair',
            'Paired notebooks',
            dict(base_path=args.pair),
        )
        click.echo(f'Finished pairing notebooks. Tip: add {args.pair!r} to '
                   'your .gitignore to keep your repository clean')
