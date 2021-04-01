"""
A product representing a File in the local filesystem with an optional client,
which allows the file to be retrieved or backed up in remote storage
"""
import warnings
import json
import shutil
import os
from pathlib import Path
from ploomber.products.Product import Product
from ploomber.placeholders.Placeholder import Placeholder
from ploomber.constants import TaskStatus
from ploomber.products.Metadata import Metadata
from reprlib import Repr


class _RemoteFile:
    """
    A product-like object to check status using remote metadata. Since it
    partially conforms to the Product API, it can use the same Metadata
    implementation (just like File). This is used to determine whether a
    task should be executed or downloaded from remote storage.

    Notes
    -----
    Must be used in a context manager
    """
    def __init__(self, file_):
        self._local_file = file_
        # download metadata from file_ to an tmp destination
        self._local_file.client.download(self._local_file._path_to_metadata,
                                         destination=self._path_to_metadata)

        self._metadata = Metadata(self)
        self._metadata._get()

    def fetch_metadata(self):
        return _fetch_metadata_from_file_product(self, check_file_exists=False)

    def exists(self):
        # This is needed to make this class compatible with Metadata.
        # Since this object is created under the assumption that the remote
        # file exists and can be downloaded, we simply return True to make
        # it compatible with the Metadata implementation
        return True

    @property
    def metadata(self):
        return self._metadata

    @property
    def _path_to_metadata(self):
        """
        Path to download remote metadata
        """
        name = f'.{self._local_file._path_to_file.name}.metadata.remote'
        return self._local_file._path_to_file.with_name(name)

    def _reset_cached_outdated_status(self):
        pass

    def _is_equal_to_local_copy(self):
        """
        Check if local metadata is the same as the remote copy
        """
        return self._local_file.metadata == self.metadata

    # TODO: _is_outdated, _outdated_code_dependency and
    # _outdated_data_dependencies are very similar to the implementations
    # in Product, check what we can abstract to avoid repetition

    def _is_outdated(self, outdated_by_code=True):
        """
        Determines outdated status using remote metadata, to decide
        whether to download the remote file or not
        """
        oudated_data = self._outdated_data_dependencies()
        outdated_code = (outdated_by_code and self._outdated_code_dependency())
        return oudated_data or outdated_code

    def _outdated_code_dependency(self):
        """
        Determine if the source code has changed by looking at the remote
        metadata
        """
        outdated, _ = self._local_file.task.dag.differ.is_different(
            self.metadata.stored_source_code,
            str(self._local_file.task.source),
            extension=self._local_file.task.source.extension)
        return outdated

    def _outdated_data_dependencies(self):
        """
        Determine if the product is outdated by checking upstream timestamps
        """
        upstream_outdated = [
            self._is_outdated_due_to_upstream(up)
            for up in self._local_file.task.upstream.values()
        ]

        # special case: if all upstream dependencies are waiting for download
        # or up-to-date, mark this as up-to-date
        if set(upstream_outdated) <= {TaskStatus.WaitingDownload, False}:
            return False

        return any(upstream_outdated)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._path_to_metadata.exists():
            self._path_to_metadata.unlink()

    def __del__(self):
        if self._path_to_metadata.exists():
            self._path_to_metadata.unlink()

    def _is_outdated_due_to_upstream(self, up):
        """
        A task becomes data outdated if an upstream product has a higher
        timestamp or if an upstream product is outdated
        """
        # if task is waiting for download, we must use the remote
        # timestamp (not the local copy) to determine status
        if up.exec_status == TaskStatus.WaitingDownload:
            with _RemoteFile(up.product) as remote_file:
                upstream_timestamp = remote_file.metadata.timestamp
        else:
            upstream_timestamp = up.product.metadata.timestamp

        if (self.metadata.timestamp is None or upstream_timestamp is None):
            return True
        else:
            return ((upstream_timestamp > self.metadata.timestamp)
                    or up.product._is_outdated())

    def __repr__(self):
        return f'{type(self).__name__}({self._local_file!r})'


class File(Product, os.PathLike):
    """A file (or directory) in the local filesystem

    Parameters
    ----------
    identifier: str or pathlib.Path
        The path to the file (or directory), can contain placeholders
        (e.g. {{placeholder}})
    """
    def __init__(self, identifier, client=None):
        super().__init__(identifier)
        self._client = client
        self._repr = Repr()
        self._repr.maxstring = 40

    def _init_identifier(self, identifier):
        if not isinstance(identifier, (str, Path)):
            raise TypeError('File must be initialized with a str or a '
                            'pathlib.Path')

        return Placeholder(str(identifier))

    @property
    def _path_to_file(self):
        return Path(str(self._identifier))

    @property
    def _path_to_metadata(self):
        name = f'.{self._path_to_file.name}.metadata'
        return self._path_to_file.with_name(name)

    def fetch_metadata(self):
        # migrate metadata file to keep compatibility with ploomber<0.10
        old_name = Path(str(self._path_to_file) + '.source')
        if old_name.is_file():
            shutil.move(old_name, self._path_to_metadata)

        return _fetch_metadata_from_file_product(self, check_file_exists=True)

    def save_metadata(self, metadata):
        self._path_to_metadata.write_text(json.dumps(metadata))

    def _delete_metadata(self):
        if self._path_to_metadata.exists():
            os.remove(str(self._path_to_metadata))

    def exists(self):
        return self._path_to_file.exists()

    def delete(self, force=False):
        # force is not used for this product but it is left for API
        # compatibility
        if self.exists():
            self.logger.debug('Deleting %s', self._path_to_file)
            if self._path_to_file.is_dir():
                shutil.rmtree(str(self._path_to_file))
            else:
                os.remove(str(self._path_to_file))
        else:
            self.logger.debug('%s does not exist ignoring...',
                              self._path_to_file)

    def __repr__(self):
        # do not shorten, we need to process the actual path
        path = Path(self._identifier.best_repr(shorten=False))

        # if absolute, try to show a shorter version, if possible
        if path.is_absolute():
            try:
                path = path.relative_to(Path('.').resolve())
            except ValueError:
                # happens if the path is not a file/folder within the current
                # working directory
                pass

        content = self._repr.repr(str(path))
        return f'{type(self).__name__}({content})'

    def _check_is_outdated(self, outdated_by_code):
        """
        Unlike other Product implementation that only have to check the
        current metadaat, File has to check if there is a metadata remote copy
        and download it to decide outdated status, which yield to task
        execution or product downloading
        """
        should_download = False

        if (self.client is not None
                and self.client._remote_exists(self._path_to_metadata)):

            # only check remote metadata if the file exists
            if self.client._remote_exists(self._path_to_file):
                with _RemoteFile(self) as remote_file:
                    if remote_file._is_equal_to_local_copy():
                        return remote_file._is_outdated()
                    else:
                        # download when doing so will bring the product
                        # up-to-date (this takes into account upstream
                        # timestamps)
                        should_download = not remote_file._is_outdated()

            else:
                warnings.warn('Found remote metadata but '
                              f'remote product {self._path_to_file} does '
                              'not exist. Ignoring remote metadata')

        if should_download:
            return TaskStatus.WaitingDownload

        # no need to download, check status using local metadata
        return super()._check_is_outdated(outdated_by_code=outdated_by_code)

    @property
    def client(self):
        if self._client is None:
            if self._task is None:
                raise ValueError('Cannot obtain client for this product, '
                                 'the constructor did not receive a client '
                                 'and this product has not been assigned '
                                 'to a DAG yet (cannot look up for clients in'
                                 'dag.clients)')

            self._client = self.task.dag.clients.get(type(self))

        return self._client

    def download(self):
        self.logger.info('Downloading %s...', self._path_to_file)

        if self.client:
            self.client.download(str(self._path_to_file))
            self.client.download(str(self._path_to_metadata))

    def upload(self):
        if self.client:
            if not self._path_to_metadata.exists():
                raise RuntimeError(
                    f'Error uploading product {self!r}. '
                    f'Metadata {str(self._path_to_metadata)!r} does '
                    'not exist')

            if not self._path_to_file.exists():
                raise RuntimeError(f'Error uploading product {self!r}. '
                                   f'Product {str(self._path_to_file)!r} does '
                                   'not exist')

            self.logger.info('Uploading %s...', self._path_to_file)
            self.client.upload(self._path_to_metadata)
            self.client.upload(self._path_to_file)

    def __fspath__(self):
        """
        Abstract method defined in the os.PathLike interface, enables this
        to work: ``import pandas as pd; pd.read_csv(File('file.csv'))``
        """
        return str(self)

    def __eq__(self, other):
        return Path(str(self)).resolve() == Path(str(other)).resolve()

    def __hash__(self):
        return hash(Path(str(self)).resolve())


def _fetch_metadata_from_file_product(product, check_file_exists):
    empty = dict(timestamp=None, stored_source_code=None)

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
            raise ValueError('Error loading JSON metadata '
                             f'for {product!r} stored at '
                             f'{str(product._path_to_metadata)!r}') from e
        else:
            # TODO: validate 'stored_source_code', 'timestamp' exist
            return parsed
    else:
        return empty
