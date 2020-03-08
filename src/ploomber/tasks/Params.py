from copy import copy
from collections import abc


class Params(abc.Mapping):
    """
    Read-only mapping to represent params passed in Task constructor. It
    initializes with a copy of the passed dictionary
    """

    def __init__(self, dict_=None):
        if dict_ is None:
            self._dict = {}
        else:

            self._dict = copy(dict_)

    def to_dict(self):
        return copy(self._dict)

    def __getitem__(self, key):
        try:
            return self._dict[key]
        except KeyError:
            raise KeyError('Cannot obtain Task param named '
                           '"{}", declared params are: {}'
                           .format(key, list(self._dict.keys())))

    def __setitem__(self, key, value):
        raise RuntimeError('Task params are read-only, if you need a copy'
                           ' use Params.to_dict() (returns a shallow copy)'
                           ' of the underlying dictionary')

    def __iter__(self):
        for name in self._dict.keys():
            yield name

    def __len__(self):
        return len(self._dict)

    def __str__(self):
        return str(self._dict)

    def __repr__(self):
        return 'Params({})'.format(repr(self._dict))

    def get(self, key):
        return self._dict.get(key)
