from inspect import isclass
from collections.abc import MutableMapping

from ploomber import tasks
from ploomber import products

_NORMALIZED_TASKS = {name.upper(): name for name in tasks.__all__}
_NORMALIZED_PRODUCTS = {name.upper(): name for name in products.__all__}
_NORMALIZED = {**_NORMALIZED_TASKS, **_NORMALIZED_PRODUCTS}

_KEY2CLASS_TASKS = {name: getattr(tasks, name) for name in tasks.__all__}
_KEY2CLASS_PRODUCTS = {
    name: getattr(products, name)
    for name in products.__all__
}
_KEY2CLASS = {**_KEY2CLASS_TASKS, **_KEY2CLASS_PRODUCTS}


def _normalize_input(name):
    return name.upper().replace('-', '').replace('_', '').replace(' ', '')


def _get_maybe(key_str):
    key_str_normalized = _normalize_input(key_str)
    return _NORMALIZED.get(key_str_normalized, None)


def _key_str_to_class(key_str):
    return _KEY2CLASS.get(key_str, None)


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
                maybe = _get_maybe(key)

                if maybe and _key_str_to_class(maybe) in self:
                    e.args = (str(e) + f'. Did you mean {maybe!r}?', )

            raise

    def __setitem__(self, key, value):
        if isinstance(key, str):
            key_str = key
            key = _key_str_to_class(key_str)

            if key is None:
                maybe = _get_maybe(key_str)

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
