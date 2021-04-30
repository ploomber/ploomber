import os
import sys
import subprocess
import shutil
from pathlib import Path
import shlex

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


class Commander:
    """A helper to write scripts
    """
    def __init__(self, workspace=None, templates_path=None):
        self.tw = TerminalWriter()
        self.workspace = None if not workspace else Path(workspace).resolve()
        self._to_delete = []

        if templates_path:
            self._env = Environment(loader=PackageLoader(*templates_path),
                                    undefined=StrictUndefined)
            self._env.filters['to_pascal_case'] = to_pascal_case
        else:
            self._env = None

    def run(self, cmd, description, capture_output=False):
        self.tw.sep('=', f'{description}: {cmd}', blue=True)
        posix = os.name == 'posix'

        # py 3.6 compatibility: cannot use subprocess.run directly
        # because the check_output arg was included until version 3.7
        if not capture_output:
            return subprocess.check_call(shlex.split(cmd, posix=posix))
        else:
            out = subprocess.check_output(shlex.split(cmd, posix=posix))
            return out.decode(sys.stdout.encoding)

    def __enter__(self):
        if self.workspace and not Path(self.workspace).exists():
            Path(self.workspace).mkdir()

        return self

    def __exit__(self, *exc):
        self.rm(*self._to_delete)

    def rm(self, *args):
        for f in args:
            _delete(f)

    def copy_template(self, path, requires_manual_edit=False, **render_kwargs):
        path = Path(path)

        if path.exists():
            self.tw.write(f'Using existing {path!s}...', green=True)
        else:
            self.tw.write(f'Missing {path!s}, adding it...')
            path.parent.mkdir(exist_ok=True, parents=True)
            content = self._env.get_template(str(path)).render(**render_kwargs)
            path.write_text(content)

            if requires_manual_edit:
                raise ValueError(f'Edit {path}')

    def cd(self, dir_):
        os.chdir(dir_)

    def cp(self, src, dir_):
        """
        Copies a file from another folder, replacing it if necessary. Used
        mainly for preparing Dockerfiles since they can only copy from the
        current working directory. Files are deleted after exiting the
        context manager
        """
        path = Path(src)

        if not path.exists():
            raise FileNotFoundError(
                f'Missing {src} file. Add it to your root folder.')

        # convert to absolute to ensure we delete the right file on __exit__
        dst = Path(dir_, path.name).resolve()
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

    # ONLY used by lambda config
    def append(self, name, dst):
        # TODO: keep a copy of the original content to restore if needed
        content = self._env.get_template(name).render()
        original = Path(dst).read_text()
        Path(dst).write_text(original + '\n' + content)
