from pathlib import PurePosixPath

try:
    from google.cloud import storage
except ImportError:
    storage = None

from ploomber.util.util import requires


class GCloudStorageClient:
    @requires(['google.cloud.storage'],
              name='GCloudStorageClient',
              pip_names=['google-cloud-storage'])
    def __init__(self, bucket_name, parent):
        storage_client = storage.Client()
        self.parent = parent
        self.bucket_name = bucket_name
        self.bucket = storage_client.bucket(bucket_name)

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
        storage_client = storage.Client()
        self.bucket = storage_client.bucket(self.bucket_name)
