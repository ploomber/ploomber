import shutil
from pathlib import Path
import stat

import click

from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import command_endpoint


def _call_in_source(dag, method_name, message, kwargs=None):
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


# TODO: --log, --log-file should not appear as options
@command_endpoint
def main():
    parser = CustomParser(description='Manage scripts and notebooks',
                          prog='ploomber nb')

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
        dag.render(show_progress=False)

    if loading_error:
        raise RuntimeError('Could not run nb command: the DAG '
                           'failed to load') from loading_error

    if args.format:
        new_paths = [
            str(p) for p in _call_in_source(
                dag,
                'format',
                'Formatted notebooks',
                dict(fmt=args.format),
            ) if p is not None
        ]

        if len(new_paths):
            click.echo('Extension changed for the following '
                       f'tasks: {", ".join(new_paths)}. Update your '
                       'pipeline declaration.')

    if args.inject:
        _call_in_source(
            dag,
            'save_injected_cell',
            'Injected celll',
            dict(),
        )

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
        click.echo(f'Finshed pairing notebooks. Tip: add {args.pair!r} to '
                   'your .gitignore to keep your repository clean')

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
