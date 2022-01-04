"""
Module for the jupyter extension


For debugging:
>>> from ploomber.jupyter.dag import JupyterDAGManager
>>> from ploomber.jupyter.manager import derive_class
>>> from jupytext.contentsmanager import TextFileContentsManager
>>> PloomberContentsManager = derive_class(TextFileContentsManager)
>>> cm = PloomberContentsManager()
"""
import sys
import datetime
import os
import contextlib
from pathlib import Path
from collections.abc import Mapping
from collections import defaultdict

from ploomber.sources.notebooksource import (_cleanup_rendered_nb, inject_cell)
from ploomber.jupyter.dag import JupyterDAGManager
from ploomber.util import loader
from ploomber.exceptions import DAGSpecInvalidError


class DAGMapping(Mapping):
    """dict-like object that maps source files to Tasks
    """
    def __init__(self, pairs):
        default = defaultdict(lambda: [])

        for name, task in pairs:
            default[name].append(task)

        self._mapping = dict(default)

    def __iter__(self):
        for k in self._mapping:
            yield k

    def __getitem__(self, key):
        """Return the first task associated with a given key
        """
        return self._mapping[key][0]

    def __len__(self):
        return len(self._mapping)

    def delete_metadata(self, key):
        """Delete metadata for all tasks associated with a given key
        """
        for task in self._mapping[key]:
            task.product.metadata.delete()

    def __repr__(self):
        return f'{type(self).__name__}({self._mapping}!r)'


@contextlib.contextmanager
def chdir(directory):
    old_dir = os.getcwd()
    try:
        os.chdir(str(directory))
        yield
    finally:
        os.chdir(old_dir)


def resolve_path(parent, path):
    """
    Functions functions resolves paths to make the {source} -> {task} mapping
    work even then `jupyter notebook` is initialized from a subdirectory
    of pipeline.yaml
    """
    try:
        # FIXME: remove :linenumber
        return Path(path).resolve().relative_to(parent).as_posix().strip()
    except ValueError:
        return None


def check_metadata_filter(log, model):
    try:
        cell_metadata_filter = (
            model['content']['metadata']['jupytext']['cell_metadata_filter'])
    except Exception:
        cell_metadata_filter = None

    if cell_metadata_filter == '-all':
        log.warning('Your notebook has filter that strips out '
                    'cell metadata when saving it from the Jupyter notebook '
                    'app. This will prevent you from tagging your '
                    '"parameters" cell. It is possible that this comes '
                    'from jupytext defaults, either add the tag by '
                    'editing the notebook in a text editor or enable '
                    'metadata in the Jupyter app: File -> Jupytext -> '
                    'Include metadata')


def derive_class(base_class):
    # FIXME: is it possible that the base class is jupyter's native contents
    # manager and we need to include jupytext as well?
    class PloomberContentsManager(base_class):
        """
        Ploomber content manager subclasses jupytext TextFileContentsManager
        to keep jupytext features of opening .py files as notebooks but adds
        a feature that automatically injects parameters in notebooks if they
        are part of a pipeline defined in pipeline.yaml, these injected
        parameters are deleted before saving the file
        """
        restart_msg = (' Fix the issue and and restart "jupyter notebook"')

        # TODO: we can cache this depending on the folder where it's called
        # all files in the same folder share the same dag
        def load_dag(self, starting_dir=None, log=True):
            if self.dag is None or self.spec['meta']['jupyter_hot_reload']:
                msg = (
                    '[Ploomber] An error occured when trying to initialize '
                    'the pipeline. Cells won\' be injected until your '
                    'pipeline processes correctly. See error details below.')

                if self.spec and not self.spec['meta']['jupyter_hot_reload']:
                    msg += self.restart_msg

                try:
                    hot_reload = (self.spec
                                  and self.spec['meta']['jupyter_hot_reload'])
                    (self.spec, self.dag,
                     path) = loader.lazily_load_entry_point(
                         starting_dir=starting_dir, reload=hot_reload)
                    path = str(Path(path).resolve())
                # this error means we couldn't locate a parent root (which is
                # required to determine which spec to use). Since it's expected
                # that this happens for some folder, we simply emit a warning
                except DAGSpecInvalidError as e:
                    self.reset_dag()
                    if log:
                        self.log.warning(
                            '[Ploomber] Skipping DAG initialization since '
                            'there isn\'t a project root in the current or '
                            'parent directories. Error message: '
                            f'{str(e)}')
                except Exception:
                    self.reset_dag()
                    if log:
                        self.log.exception(msg)
                else:
                    # dag initialized successfully...
                    # TODO: we have to remove this because the same
                    # jupyter session may more than one pipeline. should
                    # only be in sys path during pipeline loading
                    # NOTE: we only need this when we are using
                    # functions as notebooks
                    base_path = Path(path).resolve()

                    if (self.spec['meta']['jupyter_hot_reload']
                            and base_path not in sys.path):
                        # jupyter does not add the current working dir by
                        # default, if using hot reload and the dag loads
                        # functions from local files, importlib.reload will
                        # fail
                        # NOTE: might be better to only add this when the dag
                        # is actually loading from local files but that means
                        # we have to run some logic and increases load_dag
                        # running time, which we need to be fast
                        sys.path.insert(0, str(base_path))

                    render_success = True

                    with chdir(base_path):
                        # this dag object won't be executed, forcing speeds
                        # up rendering
                        try:
                            self.dag.render(force=True, show_progress=False)
                        except Exception:
                            render_success = False
                            self.reset_dag()
                            if log:
                                msg = ("[Ploomber] An error ocurred when "
                                       "rendering your DAG, cells won't be "
                                       "injected until the issue is resolved")
                                self.log.exception(msg)

                    # if rendering failed, do not continue
                    if not render_success:
                        return

                    if self.spec['meta']['jupyter_functions_as_notebooks']:
                        self.manager = JupyterDAGManager(self.dag)
                    else:
                        self.manager = None

                    pairs = [(resolve_path(
                        Path(self.root_dir).resolve(), t.source.loc), t)
                             for t in self.dag.values()
                             if t.source.loc is not None]
                    dag_mapping = DAGMapping(pairs)

                    did_keys_change = (
                        self.dag_mapping is None
                        or set(self.dag_mapping) != set(dag_mapping))

                    if did_keys_change or self.path != path:
                        self.log.info('[Ploomber] Using dag defined '
                                      f'at: {str(base_path)!r}')
                        self.log.info('[Ploomber] Pipeline mapping keys: '
                                      f'{list(dag_mapping)}')

                    self.dag_mapping = dag_mapping
                    self.path = path

        def reset_dag(self):
            self.spec = None
            self.dag = None
            self.path = None
            self.dag_mapping = None
            self.manager = None

        def __init__(self, *args, **kwargs):
            """
            Initialize the content manger, look for a pipeline.yaml file in the
            current directory, if there is one, load it, if there isn't one
            don't do anything
            """
            # NOTE: there is some strange behavior in Jupyter contents
            # manager. We should not access attributes here (e.g.,
            # self.root_dir) otherwise they aren't correctly initialized
            self.reset_dag()
            super().__init__(*args, **kwargs)

        def get(self, path, content=True, type=None, format=None):
            """
            This is called when a file/directory is requested (even in the list
            view)
            """
            # FIXME: we only need this call when functions as notebooks is true
            # FIXME: reloading inside a (functions) folder causes 404
            if content:
                # this load_dag() is required to list the folder that contains
                # the notebooks exported from functions, however, since jupyter
                # continuosly calls this for the current directory it gets
                # too verbose so we skip showing the log message
                self.load_dag(log=False)

            if self.manager and path in self.manager:
                return self.manager.get(path, content)

            model = super().get(path=path,
                                content=content,
                                type=type,
                                format=format)

            # user requested directory listing, check if there are task
            # functions defined here
            if model['type'] == 'directory' and self.manager:
                if model['content']:
                    model['content'].extend(self.manager.get_by_parent(path))

            check_metadata_filter(self.log, model)

            # if opening a file (ignore file listing), load dag again
            if (model['content'] and model['type'] == 'notebook'):
                # instantiate the dag using starting at the current folder
                self.log.info(
                    f'[Ploomber] Requested model: {model["path"]}. '
                    f'Looking for DAG with root dir: {self.root_dir}')
                self.load_dag(
                    starting_dir=Path(self.root_dir, model['path']).parent)

                if self._model_in_dag(model):
                    self.log.info('[Ploomber] Injecting cell...')
                    inject_cell(model=model,
                                params=self.dag_mapping[model['path']]._params)

            return model

        def save(self, model, path=""):
            """
            This is called when a file is saved
            """
            if self.manager and path in self.manager:
                out = self.manager.overwrite(model, path)
                return out
            else:
                check_metadata_filter(self.log, model)
                # not sure what's the difference between model['path'] and path
                # but path has leading "/", _model_in_dag strips it
                key = self._model_in_dag(model, path)

                if key:
                    content = model['content']
                    metadata = content.get('metadata', {}).get('ploomber', {})

                    if not metadata.get('injected_manually'):
                        self.log.info(
                            '[Ploomber] Cleaning up injected cell in {}...'.
                            format(model.get('name') or ''))
                        model['content'] = _cleanup_rendered_nb(content)

                    self.log.info("[Ploomber] Deleting product's metadata...")
                    self.dag_mapping.delete_metadata(key)

                return super().save(model, path)

        def _model_in_dag(self, model, path=None):
            """Determine if the model is part of the  pipeline
            """
            model_in_dag = False

            if path is None:
                path = model['path']
            else:
                path = path.strip('/')

            if self.dag:
                if ('content' in model and model['type'] == 'notebook'):
                    if path in self.dag_mapping:
                        # NOTE: not sure why sometimes the model comes with a
                        # name and sometimes it doesn't
                        self.log.info(
                            '[Ploomber] {} is part of the pipeline... '.format(
                                model.get('name') or ''))
                        model_in_dag = True
                    else:
                        self.log.info(
                            '[Ploomber] {} is not part of the pipeline, '
                            'skipping...'.format(model.get('name') or ''))

            return path if model_in_dag else False

        def list_checkpoints(self, path):
            if not self.manager or path not in self.manager:
                return self.checkpoints.list_checkpoints(path)

        def create_checkpoint(self, path):
            if not self.manager or path not in self.manager:
                return self.checkpoints.create_checkpoint(self, path)
            else:
                return {
                    'id': 'checkpoint',
                    'last_modified': datetime.datetime.now()
                }

    return PloomberContentsManager


def _load_jupyter_server_extension(app):
    """
    This function is called to configure the new content manager, there are a
    lot of quirks that jupytext maintainers had to solve to make it work so
    we base our implementation on theirs:
    https://github.com/mwouts/jupytext/blob/bc1b15935e096c280b6630f45e65c331f04f7d9c/jupytext/__init__.py#L19
    """
    if hasattr(app.contents_manager_class, 'load_dag'):
        app.log.info("[Ploomber] NotebookApp.contents_manager_class "
                     "is a subclass of PloomberContentsManager already - OK")
        return

    # The server extension call is too late!
    # The contents manager was set at NotebookApp.init_configurables

    # Let's change the contents manager class
    app.log.info('[Ploomber] setting content manager '
                 'to PloomberContentsManager')
    app.contents_manager_class = derive_class(app.contents_manager_class)

    try:
        # And re-run selected init steps from:
        # https://github.com/jupyter/notebook/blob/
        # 132f27306522b32fa667a6b208034cb7a04025c9/notebook/notebookapp.py#L1634-L1638
        app.contents_manager = app.contents_manager_class(parent=app,
                                                          log=app.log)
        app.session_manager.contents_manager = app.contents_manager
        app.web_app.settings["contents_manager"] = app.contents_manager
    except Exception:
        error = """[Ploomber] An error occured. Please
deactivate the server extension with "jupyter serverextension disable ploomber"
and configure the contents manager manually by adding
c.NotebookApp.contents_manager_class = "ploomber.jupyter.PloomberContentsManager"
to your .jupyter/jupyter_notebook_config.py file.
""" # noqa
        app.log.error(error)
        raise
