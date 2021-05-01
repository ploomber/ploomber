from pathlib import PurePosixPath, Path

try:
    from google.cloud import storage
except ImportError:
    storage = None

from ploomber.util.default import find_root_recursively
from ploomber.util.util import requires
from ploomber.clients.storage.abc import AbstractStorageClient


class GCloudStorageClient(AbstractStorageClient):
    """Client for Google Cloud Storage

    Parameters
    ----------
    bucket_name : str
        Bucket to use

    parent : str
        Parent folder in the bucket to save files

    json_credentials_path : str, default=None
        Use the given JSON file to authenticate the client
        (uses  Client.from_service_account_json(**kwargs)), if None,
        initializes the client using Client(**kwargs)

    path_to_project_root : str, default=None
        Path to project root. Product locations are stored in a path relative
        to this folder. e.g. If project root is ``/my-project``, backup is
        ``/backup`` and you save a file in ``/my-project/reports/report.html``,
        it will be saved at ``/backup/reports/report.html``. If None, it
        looks up recursively for ``environment.yml``, ``requirements.txt`` and
        ``setup.py`` (in that order) file and assigns its parent as project
        root folder.

    credentials_relative_to_project_root : bool, default=True
        If True, relative paths in json_credentials_path are so to the
        path_to_project_root, instead of the current working directory

    **kwargs
        Keyword arguments for the client constructor
    """
    @requires(['google.cloud.storage'],
              name='GCloudStorageClient',
              pip_names=['google-cloud-storage'])
    def __init__(self,
                 bucket_name,
                 parent,
                 json_credentials_path=None,
                 path_to_project_root=None,
                 credentials_relative_to_project_root=True,
                 **kwargs):
        project_root = (path_to_project_root
                        or find_root_recursively(raise_=True))
        self._path_to_project_root = Path(project_root).resolve()

        if (credentials_relative_to_project_root and json_credentials_path
                and not Path(json_credentials_path).is_absolute()):
            json_credentials_path = Path(self._path_to_project_root,
                                         json_credentials_path)

        self._from_json = json_credentials_path is not None

        if not self._from_json:
            self._client_kwargs = kwargs
        else:
            self._client_kwargs = {
                'json_credentials_path': json_credentials_path,
                **kwargs
            }

        storage_client = self._init_client()
        self._parent = parent
        self._bucket_name = bucket_name
        self._bucket = storage_client.bucket(bucket_name)

    def _init_client(self):
        constructor = (storage.Client.from_service_account_json
                       if self._from_json else storage.Client)
        return constructor(**self._client_kwargs)

    def download(self, local, destination=None):
        remote = self._remote_path(local)
        destination = destination or local

        if self._is_file(remote):
            self._download(destination, remote)
        else:
            for blob in self._bucket.client.list_blobs(self._bucket_name,
                                                       prefix=remote):
                rel = PurePosixPath(blob.name).relative_to(remote)
                destination_file = Path(destination, *rel.parts)
                destination_file.parent.mkdir(exist_ok=True, parents=True)
                blob.download_to_filename(destination_file)

    def _is_file(self, remote):
        return self._bucket.blob(remote).exists()

    def _is_dir(self, remote):
        return any(
            self._bucket.client.list_blobs(self._bucket_name, prefix=remote))

    def _download(self, local, remote):
        blob = self._bucket.blob(remote)
        Path(local).parent.mkdir(exist_ok=True, parents=True)
        blob.download_to_filename(local)

    def _upload(self, local):
        remote = self._remote_path(local)
        blob = self._bucket.blob(remote)
        blob.upload_from_filename(local)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_bucket']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        storage_client = self._init_client()
        self._bucket = storage_client.bucket(self._bucket_name)

    def __repr__(self):
        return (f'{type(self).__name__}(bucket_name={self._bucket_name!r}, '
                f'parent={self._parent!r}, '
                f'path_to_project_root={str(self._path_to_project_root)!r})')
