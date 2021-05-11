import os
import sys
import subprocess
import shutil
from pathlib import Path

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


class CommanderStop(Exception):
    """
    An exception that stops the execution of a commander without raising
    an exception
    """
    pass


class Commander:
    """A helper to write scripts
    """
    def __init__(self, workspace=None, templates_path=None):
        self.tw = TerminalWriter()
        self.workspace = None if not workspace else Path(workspace).resolve()
        self._to_delete = []

        self._wd = Path('.').resolve()

        if templates_path:
            self._env = Environment(loader=PackageLoader(*templates_path),
                                    undefined=StrictUndefined)
            self._env.filters['to_pascal_case'] = to_pascal_case
        else:
            self._env = None

    def run(self,
            *cmd,
            description=None,
            capture_output=False,
            expected_output=None,
            error_message=None,
            hint=None):
        cmd_str = ' '.join(cmd)

        if expected_output is not None and not capture_output:
            raise RuntimeError('capture_output must be True when '
                               'expected_output is not None')

        if description:
            self.tw.sep('=', f'{description}: {cmd_str}', blue=True)

        error = None

        # py 3.6 compatibility: cannot use subprocess.run directly
        # because the check_output arg was included until version 3.7
        if not capture_output:
            try:
                result = subprocess.check_call(cmd)
            except Exception as e:
                error = e
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
            cmd_str = ' '.join(cmd)
            hint = '' if not hint else f' Hint: {hint}.'
            error_message = (error_message
                             or 'An error ocurred when executing command')
            raise RuntimeError(f'({error_message} {cmd_str!r}): {error}.'
                               f'\n{hint}')
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

        return supress

    def rm(self, *args):
        for f in args:
            _delete(f)

    def copy_template(self, path, **render_kwargs):
        path = Path(path)
        dst = Path(self.workspace, path.name)

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
        os.chdir(dir_)

    def cp(self, src):
        """
        Copies a file from another folder to the workspace, replacing it if
        necessary. Used mainly for preparing Dockerfiles since they can only
        copy from the current working directory. Files are deleted after
        exiting the context manager
        """
        path = Path(src)

        if not path.exists():
            raise FileNotFoundError(
                f'Missing {src} file. Add it to your root folder.')

        # convert to absolute to ensure we delete the right file on __exit__
        dst = Path(self.workspace, path.name).resolve()
        self._to_delete.append(dst)

        _delete(dst)

        if path.is_file():
            shutil.copy(src, dst)
        else:
            shutil.copytree(src, dst)

    # ONLY used by lambda config
    def create_if_not_exists(self, name):
        if not Path(name).exists():
            content = self._env.get_template(f'default/{name}').render()
            Path(name).write_text(content)
            self._names.append(name)

    def append(self, name, dst, **kwargs):
        if not Path(dst).exists():
            Path(dst).touch()

        content = self._env.get_template(name).render(**kwargs)
        original = Path(dst).read_text()
        Path(dst).write_text(original + '\n' + content + '\n')

    def append_inline(self, line, dst):
        if not Path(dst).exists():
            Path(dst).touch()

        original = Path(dst).read_text()
        Path(dst).write_text(original + '\n' + line + '\n')

    def print(self, line):
        self.tw.write(f'{line}\n')

    def success(self, line=None):
        self.tw.sep('=', line, green=True)

    def info(self, line=None):
        self.tw.sep('=', line, blue=True)

    def warn(self, line=None):
        self.tw.sep('=', line, yellow=True)
