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
from ploomber_core.dependencies import requires
from ploomber.clients.storage.abc import AbstractStorageClient
from ploomber.exceptions import RemoteFileNotFound, DAGSpecInvalidError


class S3Client(AbstractStorageClient):
    """Client for uploading File products to Amazon S3

    Parameters
    ----------
    bucket_name : str
        Bucket to use

    parent : str
        Parent folder in the bucket to store files. For example, if
        ``parent='path/to'``, and a product in your pipeline is
        ``out/data.csv``, your file will appear in the bucket at
        ``path/to/out/data.csv``.

    json_credentials_path : str, default=None
        JSON file to authenticate the client. Must contain
        ``aws_access_key_id`` and ``aws_secret_access_key``. If None, client
        is initialized without arguments (i.e., ``boto3.client('s3')``)

    path_to_project_root : str, default=None
        Path to project root. If None, looks it
        up automatically and assigns it to the parent folder of your
        ``pipeline.yaml`` spec or ``setup.py`` (if your project is a package).
        This determines the path in remote storage. For example, if
        ``path_to_project_root`` is ``/my-project``, you're storing a
        product at ``/my-project/out/data.csv``, and ``parent='some-dir'``,
        the file will be stored in the bucket at ``some-dir/out/data.csv``
        (we first compute the path of your product relative to the project
        root, then prefix it with ``parent``).

    credentials_relative_to_project_root : bool, default=True
        If ``True``, relative paths in ``json_credentials_path`` are so to the
        ``path_to_project_root``, instead of the current working directory

    **kwargs
        Keyword arguments for the client constructor

    Examples
    --------

    Spec API:

    Given the following ``clients.py``:

    .. code-block:: python
        :class: text-editor
        :name: clients-py

        import sqlalchemy
        from ploomber.clients import S3Client

        def get():
            return S3Client(bucket_name='my-bucket', parent='my-pipeline')


    Spec API (dag-level client):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        clients:
            # all files from all tasks will be uploaded
            File: clients.get

        tasks:
            - source: notebook.ipynb
              product: output/report.html


    Spec API (dag-level client, custom arguments):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        clients:
            # if your get function takes arguments, pass them like this
            File:
                dotted_path: clients.get
                arg: value
                ...

        tasks:
            - source: notebook.ipynb
              product: output/report.html


    Spec API (product-level client):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
            - source: notebook.ipynb
              product_client: clients.get
              # outputs from this task will be uploaded
              product: output/report.html

    Python API (dag-level client):

    >>> from ploomber import DAG
    >>> from ploomber.products import File
    >>> from ploomber.tasks import PythonCallable
    >>> from ploomber.clients import S3Client
    >>> dag = DAG()
    >>> client = S3Client(bucket_name='my-bucket', parent='my-pipeline',
    ...                   path_to_project_root='.')
    >>> dag.clients[File] = client # dag-level client
    >>> dag = DAG()
    >>> def my_function(product):
    ...     Path(product).touch()
    >>> _ = PythonCallable(my_function, File('file.txt'), dag=dag)
    >>> dag.build() # doctest: +SKIP


    Python API (product-level client):

    >>> from ploomber import DAG
    >>> from ploomber.products import File
    >>> from ploomber.tasks import PythonCallable
    >>> from ploomber.clients import S3Client
    >>> dag = DAG()
    >>> client = S3Client(bucket_name='my-bucket', parent='my-pipeline',
    ...                   path_to_project_root='.')
    >>> dag = DAG()
    >>> def my_function(product):
    ...     Path(product).touch()
    >>> product = File('file.txt', client=client)
    >>> _ = PythonCallable(my_function, product, dag=dag)
    >>> dag.build() # doctest: +SKIP

    See Also
    --------
    ploomber.clients.GCloudStorageClient :
        Client for uploading products to Google Cloud Storage

    Notes
    -----
    If a notebook (or script) task fails, the partially executed ``.ipynb``
    file will be uploaded using this client.
    """

    @requires(["boto3", "botocore"], name="S3Client")
    def __init__(
        self,
        bucket_name,
        parent,
        json_credentials_path=None,
        path_to_project_root=None,
        credentials_relative_to_project_root=True,
        **kwargs,
    ):
        if path_to_project_root:
            project_root = path_to_project_root
        else:
            try:
                project_root = find_root_recursively()
            except Exception as e:
                raise DAGSpecInvalidError(
                    f"Cannot initialize {type(self).__name__} because there "
                    "is not project root. Set one or explicitly pass "
                    "a value in the path_to_project_root argument"
                ) from e

        self._path_to_project_root = Path(project_root).resolve()

        if (
            credentials_relative_to_project_root
            and json_credentials_path
            and not Path(json_credentials_path).is_absolute()
        ):
            json_credentials_path = Path(
                self._path_to_project_root, json_credentials_path
            )

        self._client_kwargs = kwargs

        if json_credentials_path:
            c = json.loads(Path(json_credentials_path).read_text())

            self._client_kwargs = {
                "aws_access_key_id": c["aws_access_key_id"],
                "aws_secret_access_key": c["aws_secret_access_key"],
                **kwargs,
            }

        self._client = self._init_client()
        self._parent = parent
        self._bucket_name = bucket_name

    def _init_client(self):
        return boto3.client("s3", **self._client_kwargs)

    def download(self, local, destination=None):
        remote = self._remote_path(local)
        destination = destination or local

        # FIXME: call _download directly and catch the exception to avoid
        # doing to api calls
        if self._is_file(remote):
            self._download(destination, remote)
        else:
            paginator = self._client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=self._bucket_name, Prefix=remote):
                if "Contents" not in page:
                    raise RemoteFileNotFound(
                        "Could not download "
                        f"{local!r} using client {self}: "
                        "No such file or directory"
                    )

                for remote_file in page["Contents"]:
                    remote_path = remote_file["Key"]
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
        resource = boto3.resource("s3", **self._client_kwargs)

        try:
            resource.Object(self._bucket_name, remote).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise
        else:
            return True

    def _is_dir(self, remote):
        bucket = boto3.resource("s3", **self._client_kwargs).Bucket(self._bucket_name)
        return any(bucket.objects.filter(Prefix=remote))

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_client"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._client = self._init_client()

    def __repr__(self):
        return (
            f"{type(self).__name__}(bucket_name={self._bucket_name!r}, "
            f"parent={self._parent!r}, "
            f"path_to_project_root={str(self._path_to_project_root)!r})"
        )

    @property
    def parent(self):
        return str(self._parent)
