import shutil
from pathlib import Path
import stat

import click

from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint


def _call_in_source(dag, method_name, kwargs=None):
    kwargs = kwargs or {}

    for task in dag.values():

        try:
            method = getattr(task.source, method_name)
        except AttributeError:
            pass

        method(**kwargs)


def _install_hook(path_to_hook, content):
    if path_to_hook.exists():
        raise RuntimeError(
            'hook already exists '
            f'at {path_to_hook}. Run: "ploomber nb -u" to uninstall the '
            'existing hook and try again')

    path_to_hook.write_text(content)
    # make the file executable
    path_to_hook.chmod(path_to_hook.stat().st_mode | stat.S_IEXEC)


def _delete_hook(path):
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
ploomber nb --remove

# re-add files
git add $(git diff --name-only --cached)
"""

post_commit_hook = """
# !/usr/bin/env bash
# Automatically generated post-commit hook to add the injected cell in
# scripts and notebook tasks

# inject cells
ploomber nb --inject
"""


# TODO: add unit tests
@cli_endpoint
def main():
    parser = CustomParser(description='Manage scripts and notebooks')

    with parser:
        cell = parser.add_mutually_exclusive_group()
        cell.add_argument('--inject',
                          '-i',
                          action='store_true',
                          help='Inject cell')
        cell.add_argument('--remove',
                          '-r',
                          action='store_true',
                          help='Remove injected cell')
        parser.add_argument('--format', '-f', help='Change format')
        parser.add_argument('--pair', '-p', help='Pair with ipynb files')
        parser.add_argument('--sync',
                            '-s',
                            action='store_true',
                            help='Sync ipynb files')

        hook = parser.add_mutually_exclusive_group()
        hook.add_argument('--install-hook',
                          '-I',
                          action='store_true',
                          help='Install git pre-commit hook')
        hook.add_argument('--uninstall-hook',
                          '-u',
                          action='store_true',
                          help='Uninstall git pre-commit hook')

    loading_error = None

    try:
        dag, args = parser.load_from_entry_point_arg()
    except Exception as e:
        loading_error = e
    else:
        dag.render()

    # NOTE: what if the pipeline.yaml isn't in the same folder as .git,
    # what if the project has multiple pipeline.yaml? - in such case, we may
    # provide a feature to discover all of them, or the user could
    # write the script
    # NOTE: maybe inject/remove should edit some project's metadata (not sure
    # if pipeline.yaml) or another file. so the Jupyter plug-in does not remove
    # the injected cell upon exiting

    if args.format:
        _call_in_source(dag, 'format', dict(fmt=args.format))

    if args.inject:
        # TODO: if paired notebooks, also inject there
        _call_in_source(dag, 'save_injected_cell', dict())

    if args.remove:
        _call_in_source(dag, 'remove_injected_cell', dict())

    if args.sync:
        # maybe its more efficient to pass all notebook paths at once?
        _call_in_source(dag, 'sync')

    # can pair give trouble if we're reformatting?
    if args.pair:
        # TODO: print a message suggesting to add the folder to .gitignore
        _call_in_source(dag, 'pair', dict(base_path=args.pair))

    if args.install_hook:
        # the generated hook should take into account the --entry-point arg
        # check .git is in this folder

        if not Path('.git').is_dir():
            raise NotADirectoryError(
                'Failed to install git hook: '
                'expected a .git/ directory in the current working directory')

        if loading_error:
            raise RuntimeError('Could not install git hook: the DAG '
                               'failed to load') from loading_error

        parent = Path('.git', 'hooks')
        parent.mkdir(exist_ok=True)

        # pre-commit: remove injected cells
        _install_hook(parent / 'pre-commit', pre_commit_hook)
        click.echo('Successfully installed pre-commit git hook')

        # post-commit: inject cells
        _install_hook(parent / 'post-commit', post_commit_hook)
        click.echo('Successfully installed post-commit git hook')

    if args.uninstall_hook:
        _delete_hook(Path('.git', 'hooks', 'pre-commit'))
        _delete_hook(Path('.git', 'hooks', 'post-commit'))
