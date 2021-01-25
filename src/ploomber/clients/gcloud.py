try:
    from google.cloud import storage
except ImportError:
    storage = None

from ploomber.util.util import requires


class GCloudStorageClient:
    @requires(['google.cloud.storage'],
              name='GCloudStorageClient',
              pip_names=['google-cloud-storage'])
    def __init__(self, bucket_name):
        storage_client = storage.Client()
        self.bucket = storage_client.bucket(bucket_name)

    def upload(self, source, dst):
        blob = self.bucket.blob(dst)
        blob.upload_from_filename(source)

    def download(self, source, dst):
        blob = self.bucket.blob(source)
        blob.download_to_filename(dst)
