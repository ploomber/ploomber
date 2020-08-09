"""
Module for the jupyter extension
"""
import os
import contextlib
from pprint import pprint
from pathlib import Path

from jupytext.contentsmanager import TextFileContentsManager

from ploomber.sources.NotebookSource import (_cleanup_rendered_nb, inject_cell)
from ploomber.spec.DAGSpec import DAGSpec
from ploomber.exceptions import DAGSpecInitializationError


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
        return str(Path(parent, path).relative_to(Path('.').resolve()))
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


class PloomberContentsManager(TextFileContentsManager):
    """
    Ploomber content manager subclasses jupytext TextFileContentsManager
    to keep jupytext features of opening .py files as notebooks but adds
    a feature that automatically injects parameters in notebooks if they
    are part of a pipeline defined in pipeline.yaml, these injected parameters
    are deleted before saving the file
    """
    restart_msg = (' Fix the issue and and restart "jupyter notebook"')

    def load_dag(self, starting_dir=None):
        if self.dag is None or self.spec['meta']['jupyter_hot_reload']:
            self.log.info('[Ploomber] Loading dag...')

            msg = ('[Ploomber] An error occured when trying to initialize '
                   'the pipeline. Cells won\' be injected until your '
                   'pipeline processes correctly. See error details below.')

            if self.spec and not self.spec['meta']['jupyter_hot_reload']:
                msg += self.restart_msg

            try:
                (self.spec, self.dag,
                 self.path) = DAGSpec.auto_load(starting_dir=starting_dir)
            except DAGSpecInitializationError:
                self.reset_dag()
                self.log.exception(msg)
            else:
                if self.dag is not None:

                    path = Path(self.path).resolve()
                    path_parent = path.parent.resolve()

                    with chdir(path_parent):
                        self.dag.render()

                    tuples = [(resolve_path(path_parent, t.source.loc), t)
                              for t in self.dag.values()
                              if t.source.loc is not None]
                    self.dag_mapping = {
                        t[0]: t[1]
                        for t in tuples if t[0] is not None
                    }

                    self.log.info('[Ploomber] Initialized dag from '
                                  'pipeline.yaml at'
                                  ': {}'.format(path))
                    self.log.info('[Ploomber] Pipeline mapping: {}'.format(
                        pprint(self.dag_mapping)))
                else:
                    # no pipeline.yaml found...
                    self.log.info('[Ploomber] No pipeline.yaml found, '
                                  'skipping DAG initialization...')
                    self.dag_mapping = None

    def reset_dag(self):
        self.spec = None
        self.dag = None
        self.path = None
        self.dag_mapping = None

    def __init__(self, *args, **kwargs):
        """
        Initialize the content manger, look for a pipeline.yaml file in the
        current directory, if there is one, load it, if there isn't one
        don't do anything
        """
        self.reset_dag()

        # try to automatically locate the dag spec
        self.load_dag()

        return super(PloomberContentsManager, self).__init__(*args, **kwargs)

    def get(self, *args, **kwargs):
        """
        This is called when a file/directory is requested (even in the list
        view)
        """
        model = super(PloomberContentsManager, self).get(*args, **kwargs)
        check_metadata_filter(self.log, model)

        # if opening a file (ignore file listing), load dag again
        if (model['content'] and model['type'] == 'notebook'):

            # Look for the pipeline.yaml file from the file we are rendering
            # and search recursively. This is required to cover the case when
            # pipeline.yaml is in a subdirectory from the folder where the
            # user executed "jupyter notebook"
            self.load_dag(starting_dir=Path(os.getcwd(), model['path']).parent)

        if self._model_in_dag(model):
            self.log.info('[Ploomber] Injecting cell...')
            inject_cell(model=model,
                        params=self.dag_mapping[model['path']]._params)

        return model

    def save(self, model, path=""):
        """
        This is called when a file is saved
        """
        check_metadata_filter(self.log, model)
        # not sure what's the difference between model['path'] and path
        # but path has leading "/", _model_in_dag strips it
        key = self._model_in_dag(model, path)

        if key:
            self.log.info(
                '[Ploomber] Cleaning up injected cell in {}...'.format(
                    model.get('name') or ''))
            model['content'] = _cleanup_rendered_nb(model['content'])

            self.log.info("[Ploomber] Deleting product's metadata...")
            self.dag_mapping[key].product.metadata.delete()

        return super(PloomberContentsManager, self).save(model, path)

    def _model_in_dag(self, model, path=None):
        """Determine if the model is part of the  pipeline
        """
        model_in_dag = False

        if path is None:
            path = model['path']
        else:
            path = path.strip('/')

        if self.dag:
            if (model['content'] and model['type'] == 'notebook'):
                if path in self.dag_mapping:
                    # NOTE: not sure why sometimes the model comes with a
                    # names and sometimes it doesn't
                    self.log.info(
                        '[Ploomber] {} is part of the pipeline... '.format(
                            model.get('name') or ''))
                    model_in_dag = True
                else:
                    self.log.info('[Ploomber] {} is not part of the pipeline, '
                                  'skipping...'.format(
                                      model.get('name') or ''))

        return path if model_in_dag else False


def _load_jupyter_server_extension(app):
    """
    This function is called to configure the new content manager, there are a
    lot of quirks that jupytext maintainers had to solve to make it work so
    we base our implementation on theirs:
    https://github.com/mwouts/jupytext/blob/bc1b15935e096c280b6630f45e65c331f04f7d9c/jupytext/__init__.py#L19
    """
    if isinstance(app.contents_manager_class, PloomberContentsManager):
        app.log.info("[Ploomber] NotebookApp.contents_manager_class "
                     "is a subclass of PloomberContentsManager already - OK")
        return

    # The server extension call is too late!
    # The contents manager was set at NotebookApp.init_configurables

    # Let's change the contents manager class
    app.log.info('[Ploomber] setting content manager '
                 'to PloomberContentsManager')
    app.contents_manager_class = PloomberContentsManager

    try:
        # And rerun selected init steps from https://github.com/jupyter/notebook/blob/
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
"""
        app.log.error(error)
        raise
