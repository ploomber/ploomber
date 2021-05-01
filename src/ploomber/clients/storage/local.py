from pathlib import Path
import shutil

from ploomber.util.default import find_root_recursively
from ploomber.clients.storage.abc import AbstractStorageClient
from ploomber.clients.storage.util import _resolve


class LocalStorageClient(AbstractStorageClient):
    """

    Parameters
    ----------
    path_to_backup_dir
        Local directory to use as backup

    path_to_project_root : str, default=None
        Path to project root. Product locations ares stored in a path relative
        to this folder. e.g. If project root is ``/my-project``, backup is
        ``/backup`` and you save a file in ``/my-project/reports/report.html``,
        it will be saved at ``/backup/reports/report.html``. If None, it
        looks up recursively for ``environment.yml``, ``requirements.txt`` and
        ``setup.py`` (in that order) file and assigns its parent as project
        root folder.
    """
    def __init__(self, path_to_backup_dir, path_to_project_root=None):
        self._path_to_backup_dir = Path(path_to_backup_dir)
        self._path_to_backup_dir.mkdir(exist_ok=True, parents=True)

        project_root = (path_to_project_root
                        or find_root_recursively(raise_=True))
        self._path_to_project_root = Path(project_root).resolve()

    def _remote_path(self, local):
        relative = _resolve(local).relative_to(self._path_to_project_root)
        return Path(self._path_to_backup_dir, relative)

    def _remote_exists(self, local):
        return self._remote_path(local).exists()

    def download(self, local, destination=None):
        remote = self._remote_path(local)
        destination = destination or local
        Path(destination).parent.mkdir(exist_ok=True, parents=True)

        if remote.is_file():
            shutil.copy(remote, destination)
        else:
            shutil.copytree(remote, destination)

    def upload(self, local):
        remote_path = self._remote_path(local)
        remote_path.parent.mkdir(exist_ok=True, parents=True)

        if Path(local).is_file():
            shutil.copy(local, remote_path)
        else:
            shutil.copytree(local, remote_path)

    def _download(self, local, remote):
        raise NotImplementedError

    def _upload(self, local):
        raise NotImplementedError

    def _is_file(self, remote):
        raise NotImplementedError

    def _is_dir(self, remote):
        raise NotImplementedError
