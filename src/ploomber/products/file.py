"""
A product representing a File in the local filesystem with an optional client,
which allows the file to be retrieved or backed up in remote storage
"""
import json
import shutil
import os
from pathlib import Path
from reprlib import Repr

from ploomber.products.product import Product
from ploomber.placeholders.placeholder import Placeholder
from ploomber.constants import TaskStatus
from ploomber.products._remotefile import (_RemoteFile,
                                           _fetch_metadata_from_file_product)
from ploomber.products.mixins import ProductWithClientMixin
from ploomber.exceptions import MissingClientError


class File(ProductWithClientMixin, os.PathLike, Product):
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
        self._remote_ = _RemoteFile(self)

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

    @property
    def _remote(self):
        """
        RemoteFile for this File. Returns None if a
        File.client doesn't exist, remote file doesn't exist or remote
        metadata doesn't exist
        """
        return self._remote_

    @property
    def _remote_path_to_metadata(self):
        return self._remote._path_to_metadata

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
        current metadata, File has to check if there is a metadata remote copy
        and download it to decide outdated status, which yield to task
        execution or product downloading
        """
        should_download = False

        if self._remote.exists():
            if self._remote._is_equal_to_local_copy():
                return self._remote._is_outdated(with_respect_to_local=True)
            else:
                # download when doing so will bring the product
                # up-to-date (this takes into account upstream
                # timestamps)
                should_download = not self._remote._is_outdated(
                    with_respect_to_local=True,
                    outdated_by_code=outdated_by_code)

        if should_download:
            return TaskStatus.WaitingDownload

        # no need to download, check status using local metadata
        return super()._check_is_outdated(outdated_by_code=outdated_by_code)

    def _is_remote_outdated(self, outdated_by_code):
        """
        Check status using remote metadata, if no remote is available
        (or remote metadata is corrupted) returns True
        """
        if self._remote.exists():
            return self._remote._is_outdated(with_respect_to_local=False,
                                             outdated_by_code=outdated_by_code)
        else:
            # if no remote, return True. This is the least destructive option
            # since we don't know what will be available and what not when this
            # executes
            return True

    @property
    def client(self):
        try:
            client = super().client
        except MissingClientError:
            return None
        else:
            return client

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
