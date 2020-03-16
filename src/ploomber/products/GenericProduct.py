"""
A generic product whose metadata is saved in a given directory and
exists/delete methods are bash commands
"""
import json
import logging
from pathlib import Path

from ploomber.products.Product import Product
from ploomber.templates.Placeholder import Placeholder


class GenericProduct(Product):
    def __init__(self, identifier, path_to_metadata, exists_command,
                 delete_command, client=None):

        self._identifier = Placeholder(str(identifier))
        self._path_to_metadata = path_to_metadata
        self._client = client

        self.exists_command = Placeholder(str(exists_command))
        self.delete_command = Placeholder(str(delete_command))

        self.did_download_metadata = False
        self.task = None
        self._logger = logging.getLogger(__name__)

    def _init_identifier(self, identifier):
        pass

    # TODO: create a mixing with this so all client-based tasks can include it
    @property
    def client(self):
        if self._client is None:
            default = self.task.dag.clients.get(type(self))

            if default is None:
                raise ValueError('{} must be initialized with a client'
                                 .format(type(self).__name__))
            else:
                self._client = default

        return self._client

    def render(self, params, **kwargs):
        # overriding parent implementation since this product also needs
        # render for other variables
        self._identifier.render(params, **kwargs)
        self.exists_command.render(params, **kwargs)
        self.delete_command.render(params, **kwargs)

    @property
    def _path_to_metadata_file(self):
        return self._path_to_metadata + str(self._identifier) + '.json'

    def fetch_metadata(self):
        try:
            meta = self.client.read_file(self._path_to_metadata_file)
        except Exception as e:
            self._logger.exception(e)
            return {}
        else:
            return json.loads(meta)

    def save_metadata(self, metadata):
        metadata_str = json.dumps(metadata)
        self.client.write_to_file(metadata_str, self._path_to_metadata_file)

    # TODO: implement
    def exists(self):
        return True

    def delete(self, force=False):
        pass

    @property
    def name(self):
        return Path(str(self._path_to_metadata)).with_suffix('').name
