"""
Products are persistent changes triggered by Tasks such as a new file
in the local filesystem or a table in a database
"""
import shutil
import os
from pathlib import Path
from ploomber.products.Product import Product
from ploomber.templates.Placeholder import Placeholder


class File(Product):
    """A file (or directory) in the local filesystem

    Parameters
    ----------
    identifier: str or pathlib.Path
        The path to the file (or directory)
    """

    def _init_identifier(self, identifier):
        if not isinstance(identifier, (str, Path)):
            raise TypeError('File must be initialized with a str or a '
                            'pathlib.Path')

        return Placeholder(str(identifier))

    @property
    def _path_to_file(self):
        return Path(str(self._identifier))

    # TODO: rename this to path_to_metadata?
    @property
    def _path_to_stored_source_code(self):
        return Path(str(self._path_to_file) + '.source')

    def fetch_metadata(self):
        # but we have no control over the stored code, it might be missing
        # so we check, we also require the file to exists: even if the
        # .source file exists, missing the actual data file means something
        # if wrong anf the task should run again
        if (self._path_to_stored_source_code.exists()
                and self._path_to_file.exists()):
            stored_source_code = self._path_to_stored_source_code.read_text()
            timestamp = self._path_to_stored_source_code.stat().st_mtime
        else:
            stored_source_code = None
            timestamp = None

        return dict(timestamp=timestamp, stored_source_code=stored_source_code)

    def save_metadata(self, metadata):
        # timestamp automatically updates when the file is saved...
        self._path_to_stored_source_code.write_text(metadata['stored_source_code'])

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

    @property
    def name(self):
        return self._path_to_file.with_suffix('').name
