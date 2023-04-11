import json

from ploomber.constants import TaskStatus
from ploomber.products.metadata import Metadata


class _RemoteFile:
    """
    A product-like object to check status using remote metadata. Since it
    partially conforms to the Product API, it can use the same Metadata
    implementation (like File). This is used to determine whether a
    task should be executed or downloaded from remote storage.

    Parameters
    ----------
    file_ : ploomber.products.File
        Product to check status

    Notes
    -----
    Must be used in a context manager
    """

    def __init__(self, file_):
        self._local_file = file_
        self._metadata = Metadata(self)

        self._is_outdated_status_local = None
        self._is_outdated_status_remote = None
        self._exists = None

    def _fetch_remote_metadata(self):
        if self.exists() and not self._metadata._did_fetch:
            self._local_file.client.download(
                self._local_file._path_to_metadata, destination=self._path_to_metadata
            )

            # load from values from file
            self._metadata._fetch()

            try:
                self._path_to_metadata.unlink()
            except FileNotFoundError:
                pass

    def exists(self):
        """
        Checks if remote File exists. This is used by Metadata to determine
        whether to use the existing remote metadat (if any) or ignore it: if
        this returns False, remote metadata is ignored even if it exists
        """
        if self._exists is None:
            # TODO remove checking if file exists and just make the API
            # call directly
            self._exists = (
                self._local_file.client is not None
                and self._local_file.client._remote_exists(
                    self._local_file._path_to_metadata
                )
                and self._local_file.client._remote_exists(
                    self._local_file._path_to_file
                )
            )

        return self._exists

    def fetch_metadata(self):
        return _fetch_metadata_from_file_product(self, check_file_exists=False)

    @property
    def metadata(self):
        self._fetch_remote_metadata()
        return self._metadata

    @property
    def _path_to_metadata(self):
        """
        Path to download remote metadata
        """
        name = f".{self._local_file._path_to_file.name}.metadata.remote"
        return self._local_file._path_to_file.with_name(name)

    def _reset_cached_outdated_status(self):
        self._is_outdated_status = None

    def _is_equal_to_local_copy(self):
        """
        Check if local metadata is the same as the remote copy
        """
        return self._local_file.metadata == self.metadata

    # TODO: _is_outdated, _outdated_code_dependency and
    # _outdated_data_dependencies are very similar to the implementations
    # in Product, check what we can abstract to avoid repetition

    def _is_outdated(self, with_respect_to_local, outdated_by_code=True):
        """
        Determines outdated status using remote metadata, to decide
        whether to download the remote file or not

        with_respect_to_local : bool
            If True, determines status by comparing timestamps with upstream
            local metadata, otherwise it uses upstream remote metadata
        """
        if with_respect_to_local:
            if self._is_outdated_status_local is None:
                self._is_outdated_status_local = self._check_is_outdated(
                    with_respect_to_local, outdated_by_code
                )
            return self._is_outdated_status_local
        else:
            if self._is_outdated_status_remote is None:
                self._is_outdated_status_remote = self._check_is_outdated(
                    with_respect_to_local, outdated_by_code
                )
            return self._is_outdated_status_remote

    def _check_is_outdated(self, with_respect_to_local, outdated_by_code):
        oudated_data = self._outdated_data_dependencies(with_respect_to_local)
        outdated_code = outdated_by_code and self._outdated_code_dependency()
        return oudated_data or outdated_code

    def _outdated_code_dependency(self):
        """
        Determine if the source code has changed by looking at the remote
        metadata
        """
        outdated, _ = self._local_file.task.dag.differ.is_different(
            a=self.metadata.stored_source_code,
            b=str(self._local_file.task.source),
            a_params=self.metadata.params,
            b_params=self._local_file.task.params.to_json_serializable(
                params_only=True
            ),
            extension=self._local_file.task.source.extension,
        )

        return outdated

    def _outdated_data_dependencies(self, with_respect_to_local):
        """
        Determine if the product is outdated by checking upstream timestamps
        """
        upstream_outdated = [
            self._is_outdated_due_to_upstream(up, with_respect_to_local)
            for up in self._local_file.task.upstream.values()
        ]

        # special case: if all upstream dependencies are waiting for download
        # or up-to-date, mark this as up-to-date
        if set(upstream_outdated) <= {TaskStatus.WaitingDownload, False}:
            return False

        return any(upstream_outdated)

    def __del__(self):
        if self._path_to_metadata.exists():
            self._path_to_metadata.unlink()

    def _is_outdated_due_to_upstream(self, upstream, with_respect_to_local):
        """
        A task becomes data outdated if an upstream product has a higher
        timestamp or if an upstream product is outdated
        """
        if (
            upstream.exec_status == TaskStatus.WaitingDownload
            or not with_respect_to_local
        ):
            # TODO: delete ._remote will never be None
            if upstream.product._remote:
                upstream_timestamp = upstream.product._remote.metadata.timestamp
            else:
                upstream_timestamp = None
        else:
            upstream_timestamp = upstream.product.metadata.timestamp

        if self.metadata.timestamp is None or upstream_timestamp is None:
            return True
        else:
            more_recent_upstream = upstream_timestamp > self.metadata.timestamp

            if with_respect_to_local:
                outdated_upstream_prod = upstream.product._is_outdated()
            else:
                outdated_upstream_prod = upstream.product._is_remote_outdated(True)

            return more_recent_upstream or outdated_upstream_prod

    def __repr__(self):
        return f"{type(self).__name__}({self._local_file!r})"


def _fetch_metadata_from_file_product(product, check_file_exists):
    if check_file_exists:
        file_exists = product._path_to_file.exists()
    else:
        file_exists = True

    # but we have no control over the stored code, it might be missing
    # so we check, we also require the file to exists: even if the
    # .metadata file exists, missing the actual data file means something
    # if wrong anf the task should run again
    if product._path_to_metadata.exists() and file_exists:
        content = product._path_to_metadata.read_text()

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(
                "Error loading JSON metadata "
                f"for {product!r} stored at "
                f"{str(product._path_to_metadata)!r}"
            ) from e
        else:
            # TODO: validate 'stored_source_code', 'timestamp' exist
            return parsed
    else:
        return None
