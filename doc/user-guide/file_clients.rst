File clients
============

File clients are used for uploading File products to the cloud. Currently two clients are supported for Amazon S3 and Google Cloud respectively.

The upload process happens in two steps:
* Given a local path, the remote path for storing the file is computed. An absolute local file path of `/path/to/project/out/data.csv` gets translated to `path/to/parent/out/data.csv`. Here, `parent` is the parent folder in the bucket to store the files.
* Upload the file using [upload_file](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file) or [upload_from_filename](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob#google_cloud_storage_blob_Blob_upload_from_filename) for Amazon S3 and Google Storage respectively.

For API docs see https://docs.ploomber.io/en/latest/api/python_api.html#clients


Pre-requisites
______________

* Create a bucket in the required cloud platform, or use an existing one.
* Configure the environment with the credentials or create a `credentials.json` file if environment is not configured.

Create a clients file
---------------------

Next, create a `clients.py` file that contains the below function for S3 client:

.. code-block:: python

    from ploomber.clients import S3Client

    def get_s3():
        return S3Client(bucket_name='bucket-name',
                        parent='parent-folder-name',
                        # pass the json_credentials_path if env not configured with credentials
                        json_credentials_path='credentials.json')

Sample file for google Cloud Storage client:

.. code-block:: python

    from ploomber.clients import GCloudStorageClient

    def get_gcloud():
        return GCloudStorageClient(bucket_name='bucket-name',
                                   parent='parent-folder-name'
                                   # pass the json_credentials_path if env not configured with credentials
                                   json_credentials_path='credentials.json')

Configure the pipeline
----------------------
Now, configure the `pipeline.yaml` file to add the `clients` key to specify the S3 or GCloud function:

.. code-block:: yaml

   tasks:
      - source: tasks.function-name
        product: output/data.parquet
   ......

   # add this
   clients:
    File: project-name.clients.get_client

    # content continues...






