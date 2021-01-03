from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
import datetime

import nbformat

from ploomber.tasks import PythonCallable


class JupyterTaskResource:
    def __init__(self, task, interactive):
        self.task = task
        self.interactive = interactive

    def to_model(self, content, path_to_dir):
        last_modified = datetime.datetime.fromtimestamp(
            filename_only(self.task.source.loc).stat().st_mtime)
        return {
            'name': self.task.name,
            'type': 'notebook',
            'content': None if not content else self.interactive.to_nb(),
            'path': '/'.join([path_to_dir, self.task.name]),
            'writable': True,
            'created': datetime.datetime.now(),
            'last_modified': last_modified,
            'mimetype': None,
            'format': 'json' if content else None,
        }


class JupyterDirectoryResource(Iterable):
    def __init__(self):
        self.task_resources = []

    def append(self, element):
        self.task_resources.append(element)

    def __iter__(self):
        for t in self.task_resources:
            yield t

    def __len__(self):
        return len(self.task_resources)

    def to_model(self, content, path_to_dir):
        return [t.to_model(content, path_to_dir) for t in self.task_resources]


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

        self.resources = defaultdict(lambda: JupyterDirectoryResource())

        for t in dag.values():
            if isinstance(t, PythonCallable):
                loc = as_jupyter_path(Path(t.source.loc).parent)
                self.resources[loc].append(
                    JupyterTaskResource(
                        task=t, interactive=t._interactive_developer()))

    def has_tasks_in_path(self, path):
        return bool(len(self.resources[path]))

    def models_in_directory(self, path_to_dir, content):
        return self.resources[path_to_dir].to_model(content, path_to_dir)

    def model_in_path(self, path, content=True):
        function_name = path.split('/')[-1]
        path_to_dir = str(PurePosixPath(path).parent)
        models = self.models_in_directory(path_to_dir, content=content)

        if models:
            models_w_name = [m for m in models if m['name'] == function_name]

            if models_w_name:
                return models_w_name[0]

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
