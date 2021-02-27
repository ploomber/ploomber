from inspect import isclass
from collections.abc import MutableMapping

from ploomber import tasks
from ploomber import products
from ploomber.validators.string import get_suggestion, str_to_class


class DAGClients(MutableMapping):
    """
    A dict-like object with validations
    1. __setitem__, __getitem__ work with strings (e.g., clients['SQLScript'])
    2. __setitem__ validates the key is a Task or Product subclass
    """
    def __init__(self, mapping=None):
        self._mapping = mapping or dict()

    def __getitem__(self, key):
        try:
            return self._mapping[key]
        except KeyError as e:
            if isinstance(key, str):
                maybe = get_suggestion(key)

                if maybe and str_to_class(maybe) in self:
                    e.args = (str(e) + f'. Did you mean {maybe!r}?', )

            raise

    def __setitem__(self, key, value):
        if isinstance(key, str):
            key_str = key
            key = str_to_class(key_str)

            if key is None:
                maybe = get_suggestion(key_str)

                msg = (f'Could not set DAG-level client {value!r}. '
                       f'{key_str!r} is not a valid Task or '
                       'Product class name')

                if maybe:
                    msg += f'. Did you mean {maybe!r}?'

                raise ValueError(msg)

        if not isclass(key) or not issubclass(
                key, (tasks.Task.Task, products.Product)):
            raise ValueError('DAG client keys must be Tasks '
                             f'or Products, value {key!r} is not')

        self._mapping[key] = value

    def __delitem__(self, key):
        del self._mapping[key]

    def __iter__(self):
        for item in self._mapping:
            yield item

    def __len__(self):
        return len(self._mapping)

    def __repr__(self):
        return f'{type(self).__name__}({self._mapping!r})'
