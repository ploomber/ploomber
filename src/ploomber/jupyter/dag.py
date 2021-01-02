from collections import defaultdict, namedtuple
from pathlib import Path
import datetime

import nbformat

from ploomber.tasks import PythonCallable

JupyterResource = namedtuple('JupyterResource', ['task', 'interactive'])


class JupyterDAGManager:
    """
    Exposes PythonCallable tasks in a dag as Jupyter notebooks
    """
    def __init__(self, dag):

        self.resources = defaultdict(lambda: [])

        for t in dag.values():
            if isinstance(t, PythonCallable):
                loc = str(Path(t.source.loc).parent)
                self.resources[loc].append(
                    JupyterResource(task=t,
                                    interactive=t._interactive_developer()))

    def has_tasks_in_path(self, path):
        return bool(len(self.resources[path]))

    def models_in_directory(self, path_to_dir, content):
        return [
            self._model(
                name=res.task.name,
                nb=None if not content else res.interactive.to_nb(),
                path=self._jupyter_path(path_to_dir, res.task.name),
                content=content,
                last_modified=datetime.datetime.fromtimestamp(
                    Path(res.task.source.loc.split(':')[0]).stat().st_mtime))
            for res in self.resources[path_to_dir]
        ]

    def model_in_path(self, path, content=True):
        path_to_function = Path('.').resolve() / path.strip('/')
        function_name = path_to_function.name
        path_to_dir = str(path_to_function.parent)
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

    def _jupyter_path(self, path, name):
        return str(Path(path).relative_to(Path('.').resolve()) / name)

    def overwrite(self, model, path):
        path_to_function = Path('.').resolve() / path.strip('/')
        function_name = path_to_function.name
        path_to_dir = str(path_to_function.parent)
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
