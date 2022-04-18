import warnings
import abc
from collections.abc import Mapping

import yaml


class Config(abc.ABC):
    """And abstract class to create configuration files (stored as YAML)

    Notes
    -----
    For examples, see test_config.py or the concrete classes
    (UserSettings, Internal)
    """
    def __init__(self):
        self._init_values()

        # resolve home directory
        path = self.path()

        if not path.exists():
            defaults = self._get_data()
            path.write_text(yaml.dump(defaults))
            self._set_data(defaults)
        else:
            text = path.read_text()

            try:
                content = yaml.safe_load(text)
                loaded = True
            except Exception as e:
                warnings.warn(f'Error loading {str(path)!r}, '
                              'reverting to default values')
                loaded = False
                content = self._get_data()

            if loaded and not isinstance(content, Mapping):
                warnings.warn(f'Error loading {str(path)!r}. Content is not'
                              'a dictionary, reverting to default values')
                content = self._get_data()

            self._set_data(content)

    def _get_data(self):
        """Extract values from the annotations and return a dictionary
        """
        return {key: getattr(self, key) for key in self.__annotations__}

    def _set_data(self, data):
        """Take a dictionary and store it in the annotations
        """
        for key in self.__annotations__:
            if key in data:
                setattr(self, key, data[key])

    def _init_values(self):
        """
        Iterate over annotations to initialize values. This is only relevant
        when any of the annotations has a factory method to initialize the
        values. If they value is a literal, no changes happen.
        """
        for key in self.__annotations__:
            name = f'{key}_default'

            # if there is a method with such name, call it and store the output
            if hasattr(self, name):
                value = getattr(self, name)()
                # call __setattr__ on the superclass so we skip the part
                # where we overwrite the YAML file, here we only want to
                # set the default values
                super().__setattr__(key, value)

    def _write(self):
        """Writes data to the YAML file
        """
        data = self._get_data()
        self.path().write_text(yaml.dump(data))

    def __setattr__(self, name, value):
        if name not in self.__annotations__:
            raise ValueError(f'{name} not a valid field')
        else:
            super().__setattr__(name, value)
            self._write()

    @abc.abstractclassmethod
    def path(cls):
        """Returns the path to the YAML file
        """
        pass
