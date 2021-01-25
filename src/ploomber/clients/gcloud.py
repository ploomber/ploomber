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
        self.bucket = storage_client.bucket(bucket_name)

    def download(self, local):
        name = PurePosixPath(local).name
        remote = str(PurePosixPath(self.parent, name))

        self._download(local, remote)

    def upload(self, local):
        name = PurePosixPath(local).name
        remote = str(PurePosixPath(self.parent, name))

        self._upload(local, remote)

    def _download(self, local, remote):
        blob = self.bucket.blob(remote)
        blob.download_to_filename(local)

    def _upload(self, local, remote):
        blob = self.bucket.blob(remote)
        blob.upload_from_filename(local)
