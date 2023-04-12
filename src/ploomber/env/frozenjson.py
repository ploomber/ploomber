from collections.abc import Mapping, MutableSequence
import keyword
import logging

import yaml


class FrozenJSON(object):
    """
    A facade for navigating a JSON-like object using attribute notation.
    Based on FrozenJSON from 'Fluent Python'
    """

    @classmethod
    def from_yaml(cls, path_to_file, *args, **kwargs):
        # load config file
        with open(path_to_file) as f:
            mapping = yaml.safe_load(f)
            # yaml.safe_load returns none if file is empty, but an empty
            # env.yaml is still useful for env.path
            mapping = mapping if mapping is not None else {}

        obj = cls(mapping, *args, **kwargs)

        # save path for reference, helps debugging
        obj._path_to_file = path_to_file

        logger = logging.getLogger(__name__)
        logger.debug("Loaded from file: {}".format(obj._path_to_file))

        return obj

    def __new__(cls, arg):
        if isinstance(arg, Mapping):
            return super(FrozenJSON, cls).__new__(cls)

        elif isinstance(arg, MutableSequence):
            return [cls(item) for item in arg]
        else:
            return arg

    def __init__(self, mapping):
        self._logger = logging.getLogger(__name__)
        self._logger.debug("Loaded with params: {}".format(mapping))
        self._path_to_file = None

        self._data = {}

        for key, value in mapping.items():
            if keyword.iskeyword(key):
                key += "_"

            self._data[key] = value

    def __getattr__(self, name):
        if hasattr(self._data, name):
            return getattr(self._data, name)
        else:
            return FrozenJSON(self._data[name])

    def __dir__(self):
        return self._data.keys()

    def __getitem__(self, key):
        value = self._data.get(key)

        if value is None:
            key_ = key if not isinstance(key, str) else "'%s'" % key
            msg = "Key error: {}, available keys are: {}".format(
                key_, self._data.keys()
            )
            if self._path_to_file is not None:
                msg += ". File loaded from {}".format(self._path_to_file)
            raise KeyError(msg)

        return value

    def __str__(self):
        if self._path_to_file:
            return self._path_to_file
        else:
            return str(self._data)

    def __repr__(self):
        return "FrozenJSON({})".format(str(self))
