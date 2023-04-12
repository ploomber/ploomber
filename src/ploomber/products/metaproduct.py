"""
Handling tasks that generate multiple products
"""
from reprlib import Repr
from collections.abc import Mapping

from ploomber.products.metadata import MetadataCollection
from ploomber.constants import TaskStatus


# TODO: raise error (or warning?) if products[*].client are different
# objects - that should not be possible
# NOTE: what do we do if metadata among products does not match?
# what if there are some products are File and others are not?
# maybe do not allow that scenario?
class ProductsContainer:
    """
    An iterator that when initialized with a sequence behaves just like it
    but when initialized with a mapping, iterates over values (instead of
    keys), this non-standard behavior but it is needed to simpplify the
    MetaProduct API since it has to work with lists and dictionaries
    """

    def __init__(self, products):
        self.products = products

        if isinstance(self.products, Mapping):
            self._first = list(self.products.values())[0]
        else:
            self._first = self.products[0]

    @property
    def first(self):
        return self._first

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
        """Returns a JSON serializable version of this product"""
        if isinstance(self.products, Mapping):
            return {name: str(product) for name, product in self.products.items()}
        else:
            return list(str(product) for product in self.products)

    def __len__(self):
        return len(self.products)

    def __repr__(self):
        return "{}({})".format(type(self).__name__, str(self.products))

    def __str__(self):
        return str(self.products)


class ClientContainer:
    def __init__(self, products):
        self._products = products

    def close(self):
        for product in self._products:
            if product.client:
                product.client.close()

    def download(self, local, destination=None):
        client = self._products.first.client
        return client.download(local, destination)


# NOTE: rename this to ProductCollection?
# TODO: create a superclass of Product, define the interface there,
# then make MetaProduct and Product inherit from it
class MetaProduct(Mapping):
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
        self.clients = ClientContainer(container)

        self._repr = Repr()

    @property
    def task(self):
        # TODO: validate same task
        return self.products[0].task

    @property
    def client(self):
        return self.clients

    @task.setter
    def task(self, value):
        for p in self.products:
            try:
                p.task = value
            except AttributeError as e:
                raise AttributeError(
                    "Expected MetaProduct to initialize with Product "
                    "instancess (which have a 'task' attribute), but "
                    f"got {p!r}, an object of type {type(p)}. Replace it "
                    "with a valid Product object. If this is a file, use "
                    f"File({p!r})"
                ) from e

    def exists(self):
        return all([p.exists() for p in self.products])

    def missing(self):
        # used to throw a meaninful error message when some products do not
        # exist after executing the user's code
        return [p for p in self.products if not p.exists()]

    def delete(self, force=False):
        for product in self.products:
            product.delete(force)

    def download(self):
        for product in self.products:
            product.download()

    def upload(self):
        for product in self.products:
            product.upload()

    def _is_outdated(self, outdated_by_code=True):
        is_outdated = [
            p._is_outdated(outdated_by_code=outdated_by_code) for p in self.products
        ]

        if set(is_outdated) == {False}:
            return False

        if set(is_outdated) <= {TaskStatus.WaitingDownload, False}:
            return TaskStatus.WaitingDownload

        return any(is_outdated)

    def _outdated_data_dependencies(self):
        return any([p._outdated_data_dependencies() for p in self.products])

    def _outdated_code_dependency(self):
        return any([p._outdated_code_dependency() for p in self.products])

    def to_json_serializable(self):
        """Returns a JSON serializable version of this product"""
        # NOTE: this is used in tasks where only JSON serializable parameters
        # are supported such as NotebookRunner that depends on papermill
        return self.products.to_json_serializable()

    def render(self, params, **kwargs):
        for p in self.products:
            p.render(params, **kwargs)

    def __repr__(self):
        content = self._repr.repr1(self.products.products, level=2)
        return f"{type(self).__name__}({content})"

    def __str__(self):
        return str(self.products)

    def __iter__(self):
        for product in self.products:
            yield product

    def __getitem__(self, key):
        return self.products[key]

    def __len__(self):
        return len(self.products)

    @property
    def _remote(self):
        return self.products.first._remote

    def _is_remote_outdated(self, outdated_by_code):
        return self.products.first._is_remote_outdated(outdated_by_code)
