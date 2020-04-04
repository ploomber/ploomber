"""
A generic product whose metadata is saved in a given directory and
exists/delete methods are bash commands
"""
import json
import logging
from pathlib import Path

from ploomber.products.Product import Product
from ploomber.products.sql import SQLiteBackedProductMixin
from ploomber.templates.Placeholder import Placeholder


# NOTE: this is gonna look a lot like SQLiteRelation, create a mixing,
# except for the exists and delete method
# TODO: add check_product and run tests: e.g .name should return a string
# no placeholders objects or {{}}
class GenericProduct(SQLiteBackedProductMixin, Product):
    """
    GenericProduct is used when there is no specific Product implementation.
    Sometimes it is technically possible to write a Product implementation
    but if you don't want to do it you can use this one. Other times is is
    not possible to provide a concrete Product implementation (e.g. we
    cannot store arbitrary metadata in a Hive table). GenericProduct works
    as any other product but its metadata is stored not in the Product itself
    but in a different backend.

    Notes
    -----
    exists does not check for product existence, just checks if metadata exists
    delete does not perform actual deletion, just deletes metadata
    """

    def __init__(self, identifier, client=None):
        super().__init__(identifier)
        self._client = client

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

    def _init_identifier(self, identifier):
        return Placeholder(identifier)

    def exists(self):
        # just check if there is metadata
        return self.fetch_metadata() is not None

    def delete(self, force=False):
        """Deletes the product
        """
        # just delete the metadata, we cannot do anything else
        pass

    @property
    def name(self):
        return str(self._identifier)
