from ploomber.clients import GCloudStorageClient


def get_storage_client(run_id):
    """Example client to upload artifacts to google cloud storage

    Parameters
    ----------
    run_id
        Run ID, all artifacts are saved in a parent folder with this name
    """
    # store credentials in a credentials.json file and add
    # google-cloud-storage to the pip section in environment.yml
    return GCloudStorageClient(bucket_name='ploomber-test-bucket',
                               parent=f'ml-basic/{run_id}',
                               json_credentials_path='credentials.json')
