from inspect import isclass
from collections.abc import MutableMapping

from ploomber.tasks.abc import Task
from ploomber.products.product import Product
from ploomber.validators.string import get_suggestion, str_to_class
from ploomber.util.dotted_path import DottedPath


class DAGClients(MutableMapping):
    """
    A dict-like object with validations
    1. __setitem__, __getitem__ work with strings (e.g., clients['SQLScript'])
    2. __setitem__ validates the key is a Task or Product subclass
    """

    def __init__(self, mapping=None):
        self._mapping = mapping or dict()

    def __getitem__(self, key):
        if isinstance(key, str):
            key_obj = str_to_class(key)
        else:
            key_obj = key

        if key_obj is None:
            error = repr(key)

            suggestion = get_suggestion(key)

            if suggestion and str_to_class(suggestion) in self:
                error += f". Did you mean {suggestion!r}?"

            raise KeyError(error)

        value = self._mapping[key_obj]

        # this happens when loading DAGSpec with lazy_load turned on,
        # clients are not initialized but passed as DottedPath
        if isinstance(value, DottedPath):
            client = value()
            self[key] = client
            return client
        else:
            return value

    def __setitem__(self, key, value):
        if isinstance(key, str):
            key_obj = str_to_class(key)

            if key_obj is None:
                maybe = get_suggestion(key)

                msg = (
                    f"Could not set DAG-level client {value!r}. "
                    f"{key!r} is not a valid Task or "
                    "Product class name"
                )

                if maybe:
                    msg += f". Did you mean {maybe!r}?"

                raise ValueError(msg)
        else:
            key_obj = key

        if not isclass(key_obj) or not issubclass(key_obj, (Task, Product)):
            raise ValueError(
                "DAG client keys must be Tasks "
                f"or Products, value {key_obj!r} is not"
            )

        self._mapping[key_obj] = value

    def __delitem__(self, key):
        del self._mapping[key]

    def __iter__(self):
        for item in self._mapping:
            yield item

    def __len__(self):
        return len(self._mapping)

    def __repr__(self):
        return f"{type(self).__name__}({self._mapping!r})"
