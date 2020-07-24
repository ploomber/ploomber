"""
Products are persistent changes triggered by Tasks such as a new file
in the local filesystem or a table in a database
"""
import warnings
import json
import shutil
import os
from pathlib import Path
from ploomber.products.Product import Product
from ploomber.placeholders.Placeholder import Placeholder


class File(Product):
    """A file (or directory) in the local filesystem

    Parameters
    ----------
    identifier: str or pathlib.Path
        The path to the file (or directory), can contain placeholders
        (e.g. {{placeholder}})
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
        empty = dict(timestamp=None, stored_source_code=None)
        # but we have no control over the stored code, it might be missing
        # so we check, we also require the file to exists: even if the
        # .source file exists, missing the actual data file means something
        # if wrong anf the task should run again
        if (self._path_to_stored_source_code.exists()
                and self._path_to_file.exists()):
            content = self._path_to_stored_source_code.read_text()

            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                # for compatibility with the previous version
                stored_source_code = content
                timestamp = self._path_to_stored_source_code.stat().st_mtime

                warnings.warn('Migrating metadata to new format...')
                loaded = dict(timestamp=timestamp,
                              stored_source_code=stored_source_code)
                self.save_metadata(loaded)
                return loaded
            else:
                # TODO: validate 'stored_source_code', 'timestamp' exist
                return parsed
        else:
            return empty

    def save_metadata(self, metadata):
        (self._path_to_stored_source_code.write_text(json.dumps(metadata)))

    def delete_metadata(self):
        if self._path_to_stored_source_code.exists():
            os.remove(str(self._path_to_stored_source_code))

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
    def suffix(self):
        return Path(self._identifier.best_str(shorten=False)).suffix
