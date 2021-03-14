import stat
import os
import csv
from datetime import datetime
import json
import shutil
from pathlib import Path
import subprocess
from collections import defaultdict

import click

from ploomber.cli.io import cli_endpoint
from ploomber.io.terminalwriter import TerminalWriter
from ploomber.Table import Table

from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.markup import MarkdownLexer
from pygments import highlight

_URL = 'https://github.com/ploomber/projects'
_DEFAULT_BRANCH = 'master'
_home = Path('~', '.ploomber')

_lexer = MarkdownLexer()
_formatter = TerminalFormatter(bg="dark")


def _delete_git_repo(path):
    """
    If on windows, we need to change permissionsto delete the repo
    """
    path_to_repo = Path(path, '.git')
    if os.name == 'nt' and path_to_repo.exists():
        for root, dirs, files in os.walk(path_to_repo):
            for dir_ in dirs:
                os.chmod(Path(root, dir_), stat.S_IRWXU)
            for file_ in files:
                os.chmod(Path(root, file_), stat.S_IRWXU)


def _display_markdown(source):
    if isinstance(source, Path):
        source = source.read_text()

    lines = source.splitlines()

    top_lines = '\n'.join(lines[:25])

    if len(lines) > 25:
        top_lines += '\n\n[...continues]'

    click.echo(highlight(top_lines, _lexer, _formatter))


def _list_examples(path):
    with open(path / 'index.csv', newline='', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))

    by_type = defaultdict(lambda: [])

    for row in rows:
        type_ = row.pop('type')
        del row['entry']
        by_type[type_].append(row)

    tw = TerminalWriter()

    tw.sep('=', 'Ploomber examples', blue=True)

    for type_ in ['basic', 'intermediate', 'advanced']:
        tw.sep(' ', type_.capitalize(), green=True)
        print(Table.from_dicts(by_type[type_]).to_format('simple'))

    tw.sep('=', blue=True)

    tw.write(f'\nTo run these examples in a hosted environment: {_URL}\n')
    tw.write('\nTo copy locally: ploomber examples -n {name}\n\n')


class _ExamplesManager:
    """Class for managing examples data
    """
    def __init__(self, home, branch=None):
        self._home = Path(home).expanduser()
        self._path_to_metadata = self._home / '.metadata'
        self._examples = self._home / 'projects'
        self._branch = branch or _DEFAULT_BRANCH

    @property
    def home(self):
        return self._home

    @property
    def examples(self):
        return self._examples

    @property
    def path_to_metadata(self):
        return self._path_to_metadata

    @property
    def branch(self):
        return self._branch

    def save_metadata(self):
        timestamp = datetime.now().timestamp()
        metadata = json.dumps(dict(timestamp=timestamp))
        self.path_to_metadata.write_text(metadata)

    def load_metadata(self):
        try:
            return json.loads(self.path_to_metadata.read_text())
        except Exception as e:
            click.echo(f'Error loading metadata: {e}')
            return None

    def clone(self):
        if not self.home.exists():
            self.home.mkdir()

        if self.examples.exists():
            _delete_git_repo(self.examples)
            shutil.rmtree(self.examples)

        try:
            subprocess.run([
                'git',
                'clone',
                '--depth',
                '1',
                '--branch',
                self.branch,
                _URL,
                str(self.examples),
            ],
                           check=True)
        except Exception as e:
            exception = e
        else:
            exception = None

        if exception:
            raise RuntimeError(
                'An error occurred when downloading examples. '
                'Verify git is installed and internet '
                f'connection. (Error message: {str(exception)!r})')

        self.save_metadata()

    def outdated(self):
        metadata = self.load_metadata()

        if metadata:
            timestamp = metadata['timestamp']
            then = datetime.fromtimestamp(timestamp)
            now = datetime.now()
            elapsed = (now - then).days
            is_outdated = elapsed >= 1

            if is_outdated:
                click.echo('Examples copy is more than 1 day old. Cloning...')

            return is_outdated
        else:
            click.echo('Cloning...')
            return True

    def path_to(self, name):
        return self.examples / name

    def path_to_readme(self):
        return self.examples / 'README.md'


@cli_endpoint
def main(name, force=False, branch=None):
    manager = _ExamplesManager(home=_home, branch=branch)
    tw = TerminalWriter()

    if not manager.examples.exists() or manager.outdated() or force:
        manager.clone()

    if not name:
        _list_examples(manager.examples)
    else:
        selected = manager.path_to(name)

        if not selected.exists():
            click.echo(f'There is no example named {name!r}. '
                       'To get available examples: ploomber examples')
        else:
            click.echo(f'Copying example to {name}/')
            shutil.copytree(selected, name)
            path_to_readme = Path(name, 'README.md')

            tw.sep('=', str(path_to_readme), blue=True)
            _display_markdown(path_to_readme)
            tw.sep('=', blue=True)
            tw.write(f'Done. Check out {path_to_readme} for details.\n',
                     green=True)
