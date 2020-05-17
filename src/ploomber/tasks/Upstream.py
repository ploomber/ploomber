from copy import copy
import warnings
from collections import defaultdict, abc
from ploomber.exceptions import UpstreamKeyError


class Upstream(abc.Mapping):
    """
    Mapping for representing upstream tasks, it's a collections.OrderedDict
    under the hood with an added .pop(key) method (like the one in a regular
    dictionary) and with a .first attribute, that returns the first value,
    useful when there is only one key-value pair.

    Used for task upstream dependencies, when employed as a context manager,
    it raises a warning if any of the elements was not used at least once,
    which means there are unused upstream dependencies

    Parameters
    ----------
    data : dict
        Data with upstream dependencies

    name : str
        Task name, only used to give more context when raising an exception
        (e.g. trying to access a key that does not exist)
    """

    def __init__(self, data=None, name=None):
        self._dict = (data or {})
        self._init_counts()
        self._in_context = False
        self._name = name

    # we cannot define this inside _init_counts or as a lambda, that does
    # not work with pickle
    def _zero():
        return 0

    def _init_counts(self):
        # lambdas do not work with pickle, must define a function
        self._counts = defaultdict(self._zero,
                                   {key: 0 for key in self._dict.keys()})

    @property
    def first(self):
        if not self._dict:
            raise KeyError('Cannot obtain first upstream task, task "{}" has '
                           'no upstream dependencies declared'
                           .format(self._name))
        if len(self._dict) > 1:
            raise ValueError('first can only be used if there is a single '
                             'upstream dependency, task "{}" has: {}'
                             .format(self._name, len(self._dict)))

        first_key = next(iter(self._dict))
        return self._dict[first_key]

    def pop(self, key):
        return self._dict.pop(key)

    def to_dict(self):
        return copy(self._dict)

    def __getitem__(self, key):
        if self._in_context:
            self._counts[key] += 1

        if not len(self._dict):
            raise UpstreamKeyError('Cannot obtain upstream dependency "{}". '
                                   'Task "{}" has no upstream dependencies'
                                   .format(key, self._name))

        try:
            return self._dict[key]
        except KeyError:
            raise UpstreamKeyError(
                'Cannot obtain upstream dependency "{}" for task "{}" '
                'declared dependencies are: {}'
                .format(key, self._name, self))

    def __setitem__(self, key, value):
        self._dict[key] = value

    def __iter__(self):
        for name in self._dict.keys():
            yield name

    def __len__(self):
        return len(self._dict)

    def __enter__(self):
        self._in_context = True
        self._init_counts()
        return self

    def __str__(self):
        return str(self._dict)

    def __repr__(self):
        return 'Upstream({})'.format(repr(self._dict))

    def __exit__(self, *exc):
        self._in_context = False
        unused = set([key for key, count in self._counts.items()
                      if count == 0])

        if unused:
            warnings.warn('The following upstream dependencies in task "{}" '
                          'were not used {}'
                          .format(self._name, unused))
