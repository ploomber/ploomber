from pathlib import Path


class PathManager:

    def __init__(self, path_to_env, env):
        self._home = Path(path_to_env).resolve().parent
        self._env = env

    @property
    def home(self):
        """Project's home folder
        """
        return self._home

    def __getattr__(self, key):
        raw_value = getattr(self._env._env_content.path, key)
        path = Path(raw_value)

        # need this if statement in case the path is a file, otherwise it will
        # try to create a folder for an existing file which throws a
        # FileExistsError error
        if not path.exists() and raw_value.endswith('/'):
            path.mkdir(parents=True, exist_ok=True)

        return path
