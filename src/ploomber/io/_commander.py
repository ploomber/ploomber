import os
import sys
import subprocess
import shutil
from pathlib import Path, PurePosixPath

from click import ClickException
from jinja2 import Environment, PackageLoader, StrictUndefined

from ploomber.io import TerminalWriter


def to_pascal_case(name):
    return ''.join([w.capitalize() for w in name.split('_')])


def _delete(dst):
    dst = Path(dst)

    if dst.is_file():
        dst.unlink()

    if dst.is_dir():
        shutil.rmtree(dst)


class CommanderException(ClickException):
    """
    Exception raised when the workflow cannot proceed and require a fix
    from the user. It is a subclass of ClickException, which signals the CLI
    to hide the traceback
    """
    pass


class CommanderStop(Exception):
    """
    An exception that stops the execution of a commander without raising
    an exception
    """
    pass


class Commander:
    """Manage script workflows
    """
    def __init__(self,
                 workspace=None,
                 templates_path=None,
                 environment_kwargs=None):
        self.tw = TerminalWriter()
        self.workspace = None if not workspace else Path(workspace).resolve()
        self._to_delete = []
        self._warnings = []

        self._wd = Path('.').resolve()

        if templates_path:
            self._env = Environment(loader=PackageLoader(*templates_path),
                                    undefined=StrictUndefined,
                                    **(environment_kwargs or {}))
            self._env.filters['to_pascal_case'] = to_pascal_case
        else:
            self._env = None

    def run(self,
            *cmd,
            description=None,
            capture_output=False,
            expected_output=None,
            error_message=None,
            hint=None,
            show_cmd=True):
        """Execute a command in a subprocess

        Parameters
        ----------
        *cmd
            Command to execute

        description: str, default=None
            Label to display before executing the command

        capture_output: bool, default=False
            Captures output, otherwise prints to standard output and standard
            error

        expected_output: str, default=None
            Raises a RuntimeError if the output is different than this value.
            Only valid when capture_output=True

        error_message: str, default=None
            Error to display when expected_output does not match. If None,
            a generic message is shown

        hint: str, default=None
            An optional string to show when at the end of the error when
            the expected_output does not match. Used to hint the user how
            to fix the problem

        show_cmd : bool, default=True
            Whether to display the command next to the description
            (and error message if it fails) or not. Only valid when
            description is not None
        """
        cmd_str = ' '.join(cmd)

        if expected_output is not None and not capture_output:
            raise RuntimeError('capture_output must be True when '
                               'expected_output is not None')

        if description:
            header = f'{description}: {cmd_str}' if show_cmd else description
            self.tw.sep('=', header, blue=True)

        error = None

        # py 3.6 compatibility: cannot use subprocess.run directly
        # because the check_output arg was included until version 3.7
        if not capture_output:
            try:
                result = subprocess.check_call(cmd)
            except Exception as e:
                error = e
        # capture outpuut
        else:
            try:
                result = subprocess.check_output(cmd)
            except Exception as e:
                error = e
            else:
                result = result.decode(sys.stdout.encoding)

            if expected_output is not None:
                error = result != expected_output

        if error:
            lines = []

            if error_message:
                line_first = error_message
            else:
                if show_cmd:
                    cmd_str = ' '.join(cmd)
                    line_first = ('An error occurred when executing '
                                  f'command: {cmd_str}')
                else:
                    line_first = 'An error occurred.'

            lines.append(line_first)

            if not capture_output:
                lines.append(f'Original error message: {error}')

            if hint:
                lines.append(f'Hint: {hint}.')

            raise CommanderException('\n'.join(lines))
        else:
            return result

    def __enter__(self):
        if self.workspace and not Path(self.workspace).exists():
            Path(self.workspace).mkdir()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # move to the original working directory
        os.chdir(self._wd)

        self.rm(*self._to_delete)
        supress = isinstance(exc_value, CommanderStop)

        if supress:
            self.info(str(exc_value))

        self._warn_show()

        return supress

    def rm(self, *args):
        """Deletes all files/directories

        Examples
        --------
        >>> cmdr.rm('file', 'directory')
        """
        for f in args:
            _delete(f)

    def rm_on_exit(self, path):
        """Removes file upon exit

        Examples
        --------
        >>> cmdr.rm_on_exit('some_temporary_file')
        """
        self._to_delete.append(Path(path).resolve())

    def copy_template(self, path, **render_kwargs):
        """Copy template to the workspace

        Parameters
        ----------
        path : str
            Path to template (relative to templates path)

        **render_kwargs
            Keyword arguments passed to the template

        Examples
        --------
        >>> # copies template in {templates-path}/directory/template.yaml
        >>> # to {workspace}/template.yaml
        >>> cmdr.copy_template('directory/template.yaml')
        """
        dst = Path(self.workspace, PurePosixPath(path).name)

        # This message is no longer valid since this is only called
        # when there is no env yet
        if dst.exists():
            self.success(f'Using existing {path!s}...')
        else:
            self.info(f'Adding {dst!s}...')
            dst.parent.mkdir(exist_ok=True, parents=True)
            content = self._env.get_template(str(path)).render(**render_kwargs)
            dst.write_text(content)

    def cd(self, dir_):
        """Change current working directory
        """
        os.chdir(dir_)

    def cp(self, src):
        """
        Copies a file or directory to the workspace, replacing it if necessary.
        Deleted on exit.

        Notes
        -----
        Used mainly for preparing Dockerfiles since they can only
        copy from the current working directory

        Examples
        --------
        >>> # copies dir/file to {workspace}/file
        >>> cmdr.cp('dir/file')
        """
        path = Path(src)

        if not path.exists():
            raise CommanderException(
                f'Missing {src} file. Add it and try again.')

        # convert to absolute to ensure we delete the right file on __exit__
        dst = Path(self.workspace, path.name).resolve()
        self._to_delete.append(dst)

        _delete(dst)

        if path.is_file():
            shutil.copy(src, dst)
        else:
            shutil.copytree(src, dst)

    def append_inline(self, line, dst):
        """Append line to a file

        Parameters
        ----------
        line : str
            Line to append

        dst : str
            File to append (can be outside the workspace)

        Examples
        --------
        >>> cmdr.append_inline('*.csv', '.gitignore')
        """
        if not Path(dst).exists():
            Path(dst).touch()

        original = Path(dst).read_text()
        Path(dst).write_text(original + '\n' + line + '\n')

    def print(self, line):
        """Print message (no color)
        """
        self.tw.write(f'{line}\n')

    def success(self, line=None):
        """Print success message (green)
        """
        self.tw.sep('=', line, green=True)

    def info(self, line=None):
        """Print information message (blue)
        """
        self.tw.sep('=', line, blue=True)

    def warn(self, line=None):
        """Print warning (yellow)
        """
        self.tw.sep('=', line, yellow=True)

    def warn_on_exit(self, line):
        """Append a warning message to be displayed on exit
        """
        self._warnings.append(line)

    def _warn_show(self):
        """Display accumulated warning messages (added via .warn_on_exit)
        """
        if self._warnings:
            self.tw.sep('=', 'Warnings', yellow=True)
            self.tw.write('\n\n'.join(self._warnings) + '\n')
            self.tw.sep('=', yellow=True)
