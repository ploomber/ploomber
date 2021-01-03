from collections import defaultdict, namedtuple
from pathlib import Path, PurePosixPath
import datetime

import nbformat

from ploomber.tasks import PythonCallable

JupyterResource = namedtuple('JupyterResource', ['task', 'interactive'])


def as_jupyter_path(path):
    """
    Paths in Jupyter are delimited by / (even on Windows) and don't have
    trailing leading slashes. This function takes a platform-dependent
    path and converts it to a valid jupyter path

    Notes
    -----
    https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html#api-paths
    """
    relative_path = Path(path).relative_to(Path('.').resolve())
    return relative_path.as_posix().strip()


def filename_only(path):
    """
    Takes a path/to/file:line path and returns path/to/file path object
    """
    parts = list(Path(path).parts)
    parts[-1] = parts[-1].split(':')[0]
    return Path(*parts)


class JupyterDAGManager:
    """
    Exposes PythonCallable tasks in a dag as Jupyter notebooks
    """
    def __init__(self, dag):

        self.resources = defaultdict(lambda: [])

        for t in dag.values():
            if isinstance(t, PythonCallable):
                loc = as_jupyter_path(Path(t.source.loc).parent)
                self.resources[loc].append(
                    JupyterResource(task=t,
                                    interactive=t._interactive_developer()))

    def has_tasks_in_path(self, path):
        return bool(len(self.resources[path]))

    def models_in_directory(self, path_to_dir, content):
        return [
            self._model(name=res.task.name,
                        nb=None if not content else res.interactive.to_nb(),
                        path='/'.join([path_to_dir, res.task.name]),
                        content=content,
                        last_modified=datetime.datetime.fromtimestamp(
                            filename_only(
                                res.task.source.loc).stat().st_mtime))
            for res in self.resources[path_to_dir]
        ]

    def model_in_path(self, path, content=True):
        function_name = path.split('/')[-1]
        path_to_dir = str(PurePosixPath(path).parent)
        models = self.models_in_directory(path_to_dir, content=content)

        if models:
            models_w_name = [m for m in models if m['name'] == function_name]

            if models_w_name:
                return models_w_name[0]

    def _model(self, name, nb, path, content, last_modified):
        return {
            'name': name,
            'type': 'notebook',
            'content': nb,
            'path': path,
            'writable': True,
            'created': datetime.datetime.now(),
            'last_modified': last_modified,
            'mimetype': None,
            'format': 'json' if content else None,
        }

    def overwrite(self, model, path):
        function_name = path.split('/')[-1]
        path_to_dir = str(PurePosixPath(path).parent)
        resources = self.resources.get(path_to_dir)

        if resources:
            resources_ = [r for r in resources if r.task.name == function_name]

            if resources_:
                resource = resources_[0]

                resource.interactive.overwrite(
                    nbformat.from_dict(model['content']))

                return {
                    'name': resource.task.name,
                    'type': 'notebook',
                    'path': path,
                    'writable': True,
                    'created': datetime.datetime.now(),
                    'last_modified': datetime.datetime.now(),
                    'content': None,
                    'mimetype': 'text/x-python',
                    'format': None,
                }
