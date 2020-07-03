from logging import NullHandler
import logging
from ploomber.dag.DAG import DAG
from ploomber.dag.DAGConfigurator import DAGConfigurator
from ploomber.env.env import Env
from ploomber.env.decorators import load_env, with_env
from ploomber.placeholders.SourceLoader import SourceLoader

from jupytext.contentsmanager import TextFileContentsManager
from ploomber.sources.NotebookSource import _cleanup_rendered_nb
from ploomber.spec.DAGSpec import auto_load
from papermill.parameterize import parameterize_notebook
import nbformat


class PloomberContentsManager(TextFileContentsManager):

    def __init__(self, *args, **kwargs):
        import sys
        sys.path.append('')
        self.log.info(sys.path)
        dag = auto_load()
        dag.render()
        self._ploomber_dag = dag
        self._mapping = {str(t.source.loc): t for t in dag.values()}
        self.log.info("Ploomber dag is {}".format(self._ploomber_dag))
        return super(PloomberContentsManager, self).__init__(*args, **kwargs)

    def get(self, *args, **kwargs):
        model = super(PloomberContentsManager, self).get(*args, **kwargs)

        if model['content'] and model['type'] == 'notebook':
            if model['path'] in self._mapping:
                self.log.info(model['content'])
                nb = nbformat.from_dict(model['content'])

                if not hasattr(nb.metadata, 'papermill'):
                    nb.metadata['papermill'] = {
                        'parameters': dict(),
                        'environment_variables': dict(),
                        'version': __version__,
                    }

                for cell in nb.cells:
                    if not hasattr(cell.metadata, 'tags'):
                        cell.metadata['tags'] = []

                params = self._mapping[model['path']]._params
                params = params.to_dict()
                params['product'] = params['product'].to_json_serializable()

                if params.get('upstream'):
                    params['upstream'] = {k: n.to_json_serializable() for k, n
                                          in params['upstream'].items()}

                model['content'] = parameterize_notebook(
                    nb, params, report_mode=False)
        return model

    def save(self, model, path=""):
        self.log.info("This is a test")
        # FIXME: clean up should also remove empty "tags"
        _cleanup_rendered_nb(model['content'])
        return super(PloomberContentsManager, self).save(model, path)


__version__ = '0.6dev'

# Set default logging handler to avoid "No handler found" warnings.

logging.getLogger(__name__).addHandler(NullHandler())


__all__ = ['DAG', 'Env', 'SourceLoader', 'load_env', 'with_env',
           'DAGConfigurator']


def _jupyter_server_extension_paths():
    return [{
        'module': 'ploomber'
    }]


def load_jupyter_server_extension(app):  # pragma: no cover
    """Use Jupytext's contents manager"""
    # if hasattr(app.contents_manager_class, "default_jupytext_formats"):
    #     app.log.info(
    #         "[Ploomber Server Extension] NotebookApp.contents_manager_class is "
    #         "(a subclass of) jupytext.TextFileContentsManager already - OK"
    #     )
    #     return

    # The server extension call is too late!
    # The contents manager was set at NotebookApp.init_configurables

    # Let's change the contents manager class
    app.log.info(
        "[Ploomber Server Extension] Deriving a JupytextContentsManager "
        "from {}".format(app.contents_manager_class.__name__)
    )
    app.contents_manager_class = PloomberContentsManager

    try:
        # And rerun selected init steps from https://github.com/jupyter/notebook/blob/
        # 132f27306522b32fa667a6b208034cb7a04025c9/notebook/notebookapp.py#L1634-L1638

        # app.init_configurables()
        app.contents_manager = app.contents_manager_class(
            parent=app, log=app.log)
        app.session_manager.contents_manager = app.contents_manager

        # app.init_components()
        # app.init_webapp()
        app.web_app.settings["contents_manager"] = app.contents_manager

        # app.init_terminals()
        # app.init_signal()

    except Exception:
        app.log.error(
            """[Ploomber Server Extension] An error occured. Please deactivate the server extension with
    jupyter serverextension disable jupytext
and configure the contents manager manually by adding
    c.NotebookApp.contents_manager_class = "jupytext.TextFileContentsManager"
to your .jupyter/jupyter_notebook_config.py file.
"""
        )
        raise
