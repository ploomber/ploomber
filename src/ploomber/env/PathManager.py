from pathlib import Path


class PathManager:

    def __init__(self, env):
        self._env = env

    def __getattr__(self, key):
        raw_value = getattr(self._env._data.path, key)
        path = Path(raw_value)

        # need this if statement in case the path is a file, otherwise it will
        # try to create a folder for an existing file which throws a
        # FileExistsError error
        if not path.exists() and raw_value.endswith('/'):
            path.mkdir(parents=True, exist_ok=True)

        return path

    def __repr__(self):
        return '{}({})'.format(type(self).__name__,
                               repr(self._env._data['path']))

    def __str__(self):
        return str(self._env._data['path'])
