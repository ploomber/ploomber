"""
A placeholder Product that doesn't do anything, used internally for
InMemoryDAG
"""
from ploomber.products import Product
from ploomber.placeholders.Placeholder import Placeholder


class EmptyProduct(Product):
    def __init__(self):
        super().__init__(identifier='')

    def _init_identifier(self, identifier):
        return Placeholder(identifier)

    def fetch_metadata(self):
        pass

    def save_metadata(self, metadata):
        pass

    def exists(self):
        pass

    def delete(self, force=False):
        pass
