import glob
import abc
from pathlib import PurePosixPath, Path

from ploomber.clients.storage.util import _resolve


class AbstractStorageClient(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def download(self, local, destination=None):
        """
        Download remote copy of a given local path. Local may be a file
        or a folder (all contents downloaded).

        Parameters
        ----------
        local
            Path to local file or folder whose remote copy will be downloaded

        destination
            Download location. If None, overwrites local copy
        """
        pass

    def upload(self, local):
        """Upload file or folder from a local path by calling _upload as needed

        Parameters
        ----------
        local
            Path to local file or folder to upload
        """
        if Path(local).is_dir():
            for f in glob.iglob(str(Path(local, "**")), recursive=True):
                if Path(f).is_file():
                    self._upload(f)
        else:
            self._upload(local)

    @abc.abstractproperty
    def parent(self):
        """Parent where all products are stored"""
        pass

    @abc.abstractmethod
    def _download(self, local, destination):
        """Download a single file"""
        pass

    @abc.abstractmethod
    def _upload(self, local):
        """Upload a single file"""
        pass

    @abc.abstractmethod
    def _is_file(self, remote):
        """Check if path to remote is file"""
        pass

    @abc.abstractmethod
    def _is_dir(self, remote):
        """Check if path to remote is a directory"""
        pass

    def _remote_path(self, local):
        """
        Given a local path, compute the remote path where the file will be
        stored.

        1. Obtain the absolute project root (``/path/to/project``)
        2. Get the local absolute path (``/path/to/project/out/data.csv``)
        3. Compute the relative path (``out/data.csv``)
        4. Prefix the relative path with the ``parent`` argument
        (passed to the Client constructor) (``path/to/parent/out/data.csv``)
        """
        relative = _resolve(local).relative_to(self._path_to_project_root)
        return str(PurePosixPath(self._parent, *relative.parts))

    def _remote_exists(self, local):
        remote = self._remote_path(local)
        is_file = self._is_file(remote)

        if is_file:
            return True

        return self._is_dir(remote)

    def close(self):
        # required to comply with the Client API
        pass
