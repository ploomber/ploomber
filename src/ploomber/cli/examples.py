import platform
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

from ploomber.io.terminalwriter import TerminalWriter
from ploomber.cli.io import command_endpoint
from ploomber.table import Table
from ploomber.telemetry import telemetry
from ploomber_core.exceptions import BaseException

from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.markup import MarkdownLexer
from pygments import highlight
from difflib import get_close_matches

_URL = "https://github.com/ploomber/projects"
_DEFAULT_BRANCH = "master"
_home = Path("~", ".ploomber")

_lexer = MarkdownLexer()
_formatter = TerminalFormatter(bg="dark")


def _find_header(md):
    """Find header markers"""
    mark = "<!-- end header -->"
    lines = md.splitlines()

    for n, line in enumerate(lines):
        if mark == line:
            return n

    return None


def _skip_header(md):
    line = _find_header(md)

    if line:
        lines = md.splitlines()
        return "\n".join(lines[line + 1 :])
    else:
        return md


def _delete_git_repo(path):
    """
    If on windows, we need to change permissionsto delete the repo
    """
    path_to_repo = Path(path, ".git")
    if os.name == "nt" and path_to_repo.exists():
        for root, dirs, files in os.walk(path_to_repo):
            for dir_ in dirs:
                os.chmod(Path(root, dir_), stat.S_IRWXU)
            for file_ in files:
                os.chmod(Path(root, file_), stat.S_IRWXU)


def _delete(source, sub):
    return source.replace(sub, "")


def _cleanup_markdown(source):
    source = _delete(source, "<!-- start description -->\n")
    source = _delete(source, "<!-- end description -->\n")
    source = _skip_header(source)
    return source


def _display_markdown(tw, path):
    LINES = 10

    source = _cleanup_markdown(path.read_text())

    lines = source.splitlines()

    top_lines = "\n".join(lines[:LINES])

    tw.write(highlight(top_lines, _lexer, _formatter))

    if len(lines) > LINES:
        tw.write(f"\n[...{str(path)} continues]\n", yellow=True)


class _ExamplesManager:
    """Listing and downloading examples

    Parameters
    ----------
    home : str, default=None
        Where to download examples. If None, it uses ~/.ploomber

    branch : str, default=None
        Branch to clone. If None, it uses 'master'

    force : bool, default=False
        If True, it forces to clone again, otherwise, it only clones every
        24 hours

    verbose : bool, default=True
        Controls verbosity
    """

    def __init__(self, home=None, branch=None, force=False, verbose=True):
        self._home = Path(home or _home).expanduser()
        self._path_to_metadata = self._home / ".metadata"
        self._examples = self._home / "projects"
        self._branch = branch or _DEFAULT_BRANCH
        self._explicit_branch = branch is not None
        self._tw = TerminalWriter()
        self._verbose = verbose

        if not self.examples.exists() or self.outdated() or force:
            if self._verbose and not self.examples.exists():
                click.echo("Local copy does not exist...")
            elif self._verbose and force:
                click.echo("Forcing download...")

            self.clone()

        with open(self.examples / "_index.csv", newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        self._examples_by_category = defaultdict(lambda: [])
        for row in rows:
            category = row.pop("category")
            del row["idx"]
            self._examples_by_category[category].append(row)
        self._example_names = [
            example["name"]
            for _, examples in self._examples_by_category.items()
            for example in examples
        ]

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

    def save_metadata(self, branch):
        timestamp = datetime.now().timestamp()
        metadata = json.dumps(dict(timestamp=timestamp, branch=branch))
        self.path_to_metadata.write_text(metadata)

    def load_metadata(self):
        try:
            return json.loads(self.path_to_metadata.read_text())
        except Exception as e:
            click.echo(f"Error loading metadata: {e}")
            return None

    def clone(self):
        if not self.home.exists():
            self.home.mkdir()

        if self.examples.exists():
            _delete_git_repo(self.examples)
            shutil.rmtree(self.examples)

        try:
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "--branch",
                    self.branch,
                    _URL,
                    str(self.examples),
                ],
                check=True,
            )
        except Exception as e:
            exception = e
        else:
            exception = None

        if exception:
            raise RuntimeError(
                "An error occurred when downloading examples. "
                "Verify git is installed and your internet "
                f"connection. (Error message: {str(exception)!r})"
            )

        self.save_metadata(branch=self.branch)

    def outdated(self):
        metadata = self.load_metadata()

        if metadata:
            timestamp = metadata["timestamp"]
            then = datetime.fromtimestamp(timestamp)
            now = datetime.now()
            elapsed = (now - then).days
            is_more_than_one_day_old = elapsed >= 1

            is_different_branch = metadata.get("branch") != self.branch

            if is_more_than_one_day_old:
                click.echo("Examples copy is more than 1 day old...")

            if is_different_branch and self._explicit_branch:
                click.echo("Different branch requested...")

            return is_more_than_one_day_old or (
                is_different_branch and self._explicit_branch
            )
        else:
            click.echo("Cloning...")
            return True

    def path_to(self, name):
        return self.examples / name

    def path_to_readme(self):
        return self.examples / "README.md"

    def _suggest_example(self, name: str, options: list):
        if not name or name in options:
            return None
        name = name.lower()
        close_commands = get_close_matches(name, options)
        if close_commands:
            return close_commands[0]
        else:
            return None

    def list(self):
        categories = json.loads((self.examples / "_category.json").read_text())

        tw = TerminalWriter()

        click.echo(f"Branch: {self.branch}")
        tw.sep("=", "Ploomber examples", blue=True)
        click.echo()

        for category in sorted(self._examples_by_category):
            title = category.capitalize()
            description = categories.get(category)

            if description:
                title = f"{title} ({description})"

            tw.sep(" ", title, green=True)
            click.echo()
            click.echo(
                Table.from_dicts(self._examples_by_category[category]).to_format(
                    "simple"
                )
            )
            click.echo()

        tw.sep("=", blue=True)

        tw.write(
            "\nTo run these examples in free, hosted "
            f"environment, see instructions at: {_URL}"
        )
        tw.write("\nTo download: ploomber examples -n name -o path\n")
        tw.write("Example: ploomber examples -n templates/ml-basic -o ml\n\n")

    def download(self, name, output):
        selected = self.path_to(name)

        if not selected.exists():
            closest_match = self._suggest_example(name, self._example_names)
            # when suggested command returns None, disable did you mean feature
            if closest_match is not None:
                raise BaseException(
                    f"There is no example named {name!r}. "
                    f'Did you mean "{closest_match}"?',
                    type_="no-example-with-name",
                )
            else:
                print()
                raise BaseException(
                    f"There is no example named {name!r}. \n"
                    "List examples: ploomber examples\n"
                    "Update local copy: ploomber examples -f\n"
                    "Get ML example: ploomber examples -n "
                    "templates/ml-basic -o ml-example",
                    type_="no-example-with-name",
                )
        else:
            output = output or name

            if self._verbose:
                self._tw.sep("=", f"Copying example {name} to {output}/", blue=True)

            if Path(output).exists():
                raise BaseException(
                    f"{output!r} already exists in the current working "
                    "directory, please rename it or move it "
                    "to another location and try again.",
                    type_="directory-exists",
                )

            shutil.copytree(selected, output)

            path_to_readme = Path(output, "README.md")
            out_dir = output + ("\\" if platform.system() == "Windows" else "/")

            if self._verbose:
                self._tw.write(
                    "Next steps:\n\n" f"$ cd {out_dir}" f"\n$ ploomber install"
                )
                self._tw.write(
                    f"\n\nOpen {str(path_to_readme)} for details.\n", blue=True
                )


@command_endpoint
@telemetry.log_call("examples")
def main(name, force=False, branch=None, output=None):
    """
    Entry point for examples
    """
    examples_manager = _ExamplesManager(branch=branch, force=force, verbose=True)
    if not name:
        examples_manager.list()
    else:
        examples_manager.download(name=name, output=output)
