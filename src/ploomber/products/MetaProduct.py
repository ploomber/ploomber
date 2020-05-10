from collections.abc import Mapping

from ploomber.products.Metadata import MetadataCollection


class ProductsContainer:
    """
    An iterator that when initialized with a sequence behaves just like it
    but when initialized with a mapping, iterates over values (instead of
    keys), this non-standard behavior but it is needed to simpplify the
    MetaProduct API since it has to work with lists and dictionaries
    """

    def __init__(self, products):
        self.products = products

    def __iter__(self):
        if isinstance(self.products, Mapping):
            for product in self.products.values():
                yield product
        else:
            for product in self.products:
                yield product

    def __getitem__(self, key):
        return self.products[key]

    def to_json_serializable(self):
        """Returns a JSON serializable version of this product
        """
        if isinstance(self.products, Mapping):
            return {name: str(product) for name, product
                    in self.products.items()}
        else:
            return list(str(product) for product in self.products)

    def __len__(self):
        return len(self.products)

    def __repr__(self):
        return '{}: {}'.format(type(self).__name__, repr(self.products))

    def __str__(self):
        return str(self.products)


# NOTE: rename this to ProductCollection?
class MetaProduct:
    """
    Exposes a Product-like API to allow Tasks to create more than one Product,
    it is automatically instantiated when a Task is initialized with a
    sequence or a mapping object in the product parameter. While it is
    recommended for Tasks to only have one Product (to keep them simple),
    in some cases it makes sense. For example, a Jupyter notebook
    (executed via NotebookRunner), for fitting a model might as well serialize
    the things such as the model and any data preprocessors
    """

    def __init__(self, products):
        container = ProductsContainer(products)

        self.products = container
        self.metadata = MetadataCollection(container)

    @property
    def timestamp(self):
        return self.metadata.timestamp

    @property
    def stored_source_code(self):
        return self.metadata.stored_source_code

    @property
    def task(self):
        # TODO: validate same task
        return self.products[0].task

    @task.setter
    def task(self, value):
        for p in self.products:
            p.task = value

    @timestamp.setter
    def timestamp(self, value):
        for p in self.products:
            p.metadata['timestamp'] = value

    @stored_source_code.setter
    def stored_source_code(self, value):
        for p in self.products:
            p.metadata['stored_source_code'] = value

    def exists(self):
        return all([p.exists() for p in self.products])

    def delete(self, force=False):
        for product in self.products:
            product.delete(force)

    def _is_outdated(self, outdated_by_code=True):
        return any([p._is_outdated(outdated_by_code=outdated_by_code)
                    for p in self.products])

    def _outdated_data_dependencies(self):
        return any([p._outdated_data_dependencies()
                    for p in self.products])

    def _outdated_code_dependency(self):
        return any([p._outdated_code_dependency()
                    for p in self.products])

    def _clear_cached_status(self):
        for p in self.products:
            p._clear_cached_status()

    def to_json_serializable(self):
        """Returns a JSON serializable version of this product
        """
        # NOTE: this is used in tasks where only JSON serializable parameters
        # are supported such as NotebookRunner that depends on papermill
        return self.products.to_json_serializable()

    def _save_metadata(self, source_code):
        self.metadata.update(source_code)

    def render(self, params, **kwargs):
        for p in self.products:
            p.render(params, **kwargs)

    def __repr__(self):
        return '{}: {}'.format(type(self).__name__, str(self.products))

    def __str__(self):
        return str(self.products)

    def __iter__(self):
        for product in self.products:
            yield product

    def __getitem__(self, key):
        return self.products[key]

    def get(self, key):
        try:
            return self[key]
        except KeyError:
            return None

    def __len__(self):
        return len(self.products)
