import abc
from collections.abc import Mapping

import yaml


class Config(abc.ABC):
    def __init__(self):
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
            except Exception:
                # NOTE: show warning?
                loaded = False
                content = self._get_data()

            if loaded and not isinstance(content, Mapping):
                # NOTE: show warning?
                content = self._get_data()

            self._set_data(content)

    # TODO: delete, only here for compatibility
    def read(self):
        return self._get_data()

    def _get_data(self):
        return {key: getattr(self, key) for key in self.__annotations__}

    def _set_data(self, data):
        for key in self.__annotations__:
            if key in data:
                setattr(self, key, data[key])

    def _write(self):
        path = self.path()
        data = self._get_data()
        path.write_text(yaml.dump(data))

    def __setattr__(self, name, value):
        if name not in self.__annotations__:
            raise ValueError(f'{name} not a valid field')
        else:
            super().__setattr__(name, value)
            self._write()

    @abc.abstractclassmethod
    def path(cls):
        pass
