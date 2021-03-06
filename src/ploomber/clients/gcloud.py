from pathlib import PurePosixPath

try:
    from google.cloud import storage
except ImportError:
    storage = None

from ploomber.util.util import requires


class GCloudStorageClient:
    """Client for Google Cloud Storage

    Parameters
    ----------
    bucket_name : str
        Bucket to use

    parent : str
        Parent folder to save files

    json_credentials_path : str, default=None
        Use the given JSON file to authenticate the client
        (uses  Client.from_service_account_json(**kwargs)), if None,
        initializes the client using Client(**kwargs)

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
                 **kwargs):
        self._from_json = json_credentials_path is not None

        if not self._from_json:
            self._client_kwargs = kwargs
        else:
            self._client_kwargs = {
                'json_credentials_path': json_credentials_path,
                **kwargs
            }

        storage_client = self._init_client()
        self.parent = parent
        self.bucket_name = bucket_name
        self.bucket = storage_client.bucket(bucket_name)

    def _init_client(self):
        constructor = (storage.Client.from_service_account_json
                       if self._from_json else storage.Client)
        return constructor(**self._client_kwargs)

    def download(self, local):
        remote = self._remote_path(local)
        self._download(local, remote)

    def upload(self, local):
        remote = self._remote_path(local)
        self._upload(local, remote)

    def close(self):
        pass

    def _remote_path(self, local):
        name = PurePosixPath(local).name
        return str(PurePosixPath(self.parent, name))

    def _remote_exists(self, local):
        remote = self._remote_path(local)
        return self.bucket.blob(remote).exists()

    def _download(self, local, remote):
        blob = self.bucket.blob(remote)
        blob.download_to_filename(local)

    def _upload(self, local, remote):
        blob = self.bucket.blob(remote)
        blob.upload_from_filename(local)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['bucket']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        storage_client = self._init_client()
        self.bucket = storage_client.bucket(self.bucket_name)
