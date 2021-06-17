from pathlib import Path, PurePosixPath
import datetime

import nbformat

from ploomber.tasks import PythonCallable


class JupyterTaskResource:
    def __init__(self, task, interactive, parent):
        self.task = task
        self.interactive = interactive
        self.path = '/'.join([parent, self.task.name])

    def to_model(self, content=False):
        last_modified = datetime.datetime.fromtimestamp(
            remove_line_number(self.task.source.loc).stat().st_mtime)
        return {
            'name': self.task.name,
            'type': 'notebook',
            'content': None if not content else self.interactive.to_nb(),
            'path': self.path,
            'writable': True,
            'created': datetime.datetime.now(),
            'last_modified': last_modified,
            'mimetype': None,
            'format': 'json' if content else None,
        }

    def __repr__(self):
        return (f'{type(self).__name__}(name={self.task.name!r}, '
                f'path={self.path})')


class JupyterDirectoryResource:
    def __init__(self, name, path):
        self.task_resources = dict()
        self.name = name
        self.path = path

    def __setitem__(self, key, value):
        self.task_resources[key] = value

    def get(self, key):
        return self.task_resources.get(key)

    def __iter__(self):
        for t in self.task_resources:
            yield t

    def __len__(self):
        return len(self.task_resources)

    def to_model(self, content=False):
        content = [
            t.to_model(content=False) for t in self.task_resources.values()
        ]
        created = min(c['created'] for c in content)
        last_modified = max(c['last_modified'] for c in content)
        return {
            'name': self.name,
            'path': self.path,
            'type': 'directory',
            'created': created,
            'last_modified': last_modified,
            'format': 'json',
            'mimetype': None,
            'content': content,
            'writable': True,
        }

    def __repr__(self):
        return (f'{type(self).__name__}(name={self.name!r}, '
                f'path={self.path}, '
                f'task_resources={self.task_resources!r})')


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
    return relative_path.as_posix().strip('/')


def remove_line_number(path):
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
        self.resources = dict()

        for t in dag.values():
            if isinstance(t, PythonCallable):
                loc = remove_line_number(t.source.loc)
                name = loc.name + ' (functions)'
                key = str(PurePosixPath(as_jupyter_path(loc)).with_name(name))

                if key not in self.resources:
                    self.resources[key] = JupyterDirectoryResource(name=name,
                                                                   path=key)

                task_resource = JupyterTaskResource(
                    task=t, interactive=t._interactive_developer(), parent=key)
                self.resources[key][t.name] = task_resource
                # self.resources_keys.append(task_resource.path)
                self.resources[task_resource.path] = task_resource

        pairs = ((str(PurePosixPath(path).parent), res)
                 for path, res in self.resources.items())

        self.resources_by_root = dict()

        for parent, resource in pairs:
            if parent not in self.resources_by_root:
                self.resources_by_root[parent] = []

            self.resources_by_root[parent].append(resource)

    def __contains__(self, key):
        return key.strip('/') in self.resources

    def __getitem__(self, key):
        return self.resources[key]

    def __iter__(self):
        for resource in self.resources:
            yield resource

    def _get(self, path):
        path = path.strip('/')

        return self.resources.get(path)

    def get(self, path, content):
        """Get model located at path
        """
        resource = self._get(path)
        if resource:
            return resource.to_model(content)

    def get_by_parent(self, parent):
        parent = parent.strip('/')

        # jupyter represents the current folder with an empty string
        if parent == '':
            parent = '.'

        if parent in self.resources_by_root:
            return [m.to_model() for m in self.resources_by_root[parent]]
        else:
            return []

    def overwrite(self, model, path):
        """Overwrite a model back to the original function
        """
        resource = self._get(path)

        resource.interactive.overwrite(nbformat.from_dict(model['content']))

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

    def __repr__(self):
        return f'{type(self).__name__}({list(self.resources)})'
