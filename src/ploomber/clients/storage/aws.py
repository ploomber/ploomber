import json
from pathlib import PurePosixPath, Path

try:
    import boto3
except ImportError:
    boto3 = None

try:
    import botocore
except ImportError:
    botocore = None

from ploomber.util.default import find_root_recursively
from ploomber.util.util import requires
from ploomber.clients.storage.abc import AbstractStorageClient
from ploomber.exceptions import RemoteFileNotFound, DAGSpecInvalidError


class S3Client(AbstractStorageClient):
    """Client for Amazon S3

    Parameters
    ----------
    bucket_name : str
        Bucket to use

    parent : str
        Parent folder in the bucket to save files

    json_credentials_path : str, default=None
        Use the given JSON file to authenticate the client. Must contain
        aws_access_key_id and aws_secret_access_key.
        If None, client is initialized without arguments

    path_to_project_root : str, default=None
        Path to project root. Product locations are stored in a path relative
        to this folder. e.g. If project root is ``/my-project``, backup is
        ``/backup`` and you save a file in ``/my-project/reports/report.html``,
        it will be saved at ``/backup/reports/report.html``. If None, looks it
        up automatically and assigns it to the parent folder of the root YAML
        spec ot setup.py (if your project is a package).

    credentials_relative_to_project_root : bool, default=True
        If True, relative paths in json_credentials_path are so to the
        path_to_project_root, instead of the current working directory

    **kwargs
        Keyword arguments for the client constructor
    """
    @requires(['boto3', 'botocore'], name='S3Client')
    def __init__(self,
                 bucket_name,
                 parent,
                 json_credentials_path=None,
                 path_to_project_root=None,
                 credentials_relative_to_project_root=True,
                 **kwargs):

        if path_to_project_root:
            project_root = path_to_project_root
        else:
            try:
                project_root = find_root_recursively()
            except Exception as e:
                raise DAGSpecInvalidError(
                    f'Cannot initialize {type(self).__name__} because there '
                    'is not project root. Set one or explicitly pass '
                    'a value in the path_to_project_root argument') from e

        self._path_to_project_root = Path(project_root).resolve()

        if (credentials_relative_to_project_root and json_credentials_path
                and not Path(json_credentials_path).is_absolute()):
            json_credentials_path = Path(self._path_to_project_root,
                                         json_credentials_path)

        self._client_kwargs = kwargs

        if json_credentials_path:
            c = json.loads(Path(json_credentials_path).read_text())

            self._client_kwargs = {
                'aws_access_key_id': c['aws_access_key_id'],
                'aws_secret_access_key': c['aws_secret_access_key'],
                **kwargs
            }

        self._client = self._init_client()
        self._parent = parent
        self._bucket_name = bucket_name

    def _init_client(self):
        return boto3.client('s3', **self._client_kwargs)

    def download(self, local, destination=None):
        remote = self._remote_path(local)
        destination = destination or local

        # FIXME: call _download directly and catch the exception to avoid
        # doing to api calls
        if self._is_file(remote):
            self._download(destination, remote)
        else:
            paginator = self._client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self._bucket_name,
                                           Prefix=remote):
                if 'Contents' not in page:
                    raise RemoteFileNotFound('Could not download '
                                             f'{local!r} using client {self}: '
                                             'No such file or directory')

                for remote_file in page['Contents']:
                    remote_path = remote_file['Key']
                    rel = PurePosixPath(remote_path).relative_to(remote)
                    destination_file = Path(destination, *rel.parts)
                    destination_file.parent.mkdir(exist_ok=True, parents=True)
                    self._download(str(destination_file), remote_path)

    def _upload(self, local):
        remote = self._remote_path(local)
        self._client.upload_file(str(local), self._bucket_name, remote)

    def _download(self, local, remote):
        Path(local).parent.mkdir(exist_ok=True, parents=True)
        self._client.download_file(self._bucket_name, remote, str(local))

    def _is_file(self, remote):
        resource = boto3.resource('s3', **self._client_kwargs)

        try:
            resource.Object(self._bucket_name, remote).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise
        else:
            return True

    def _is_dir(self, remote):
        bucket = boto3.resource('s3', **self._client_kwargs).Bucket(
            self._bucket_name)
        return any(bucket.objects.filter(Prefix=remote))

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_client']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._client = self._init_client()

    def __repr__(self):
        return (f'{type(self).__name__}(bucket_name={self._bucket_name!r}, '
                f'parent={self._parent!r}, '
                f'path_to_project_root={str(self._path_to_project_root)!r})')
