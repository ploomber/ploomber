"""
Module for the jupyter extension
"""
from jupytext.contentsmanager import TextFileContentsManager
from ploomber.sources.NotebookSource import _cleanup_rendered_nb
from ploomber.spec.DAGSpec import DAGSpec
from papermill.parameterize import parameterize_notebook
import nbformat
from papermill import __version__


class PloomberContentsManager(TextFileContentsManager):
    """
    Ploomber content manager subclasses jupytext TextFileContentsManager
    to keep jupytext features of opening .py files as notebooks but adds
    a feature that automatically injects parameters in notebooks if they
    are part of a pipeline defined in pipeline.yaml, these injected parameters
    are deleted before saving the file
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the content manger, look for a pipeline.yaml file in the
        current directory, if there is one, load it, if there isn't one
        don't do anything
        """
        import sys
        sys.path.append('')
        self.log.info(sys.path)
        dag = DAGSpec.auto_load()
        dag.render()
        self._ploomber_dag = dag
        self._mapping = {str(t.source.loc): t for t in dag.values()}
        self.log.info("Ploomber dag is {}".format(self._ploomber_dag))
        return super(PloomberContentsManager, self).__init__(*args, **kwargs)

    def get(self, *args, **kwargs):
        """
        This is called when a file/directory is requested (even in the list
        view)
        """
        model = super(PloomberContentsManager, self).get(*args, **kwargs)

        # FIXME: if there is no dag, don't do anything

        if model['content'] and model['type'] == 'notebook':
            if model['path'] in self._mapping:
                self.log.info(model['content'])
                nb = nbformat.from_dict(model['content'])

                # papermill adds a bunch of things before calling
                # parameterize_notebook
                # https://github.com/nteract/papermill/blob/0532d499e13e93d8990211be33e9593f1bffbe6c/papermill/iorw.py#L400
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
        """
        This is called when a file is saved
        """
        # FIXME: if there is no dag, don't do anything
        self.log.info("This is a test")
        # FIXME: clean up should also remove empty "tags"
        _cleanup_rendered_nb(model['content'])
        return super(PloomberContentsManager, self).save(model, path)


def _load_jupyter_server_extension(app):
    """
    This function is called to configure the new content manager, there are a
    lot of quirks that jupytext maintainers had to solve to make it work so
    we base our implementation on theirs:
    https://github.com/mwouts/jupytext/blob/bc1b15935e096c280b6630f45e65c331f04f7d9c/jupytext/__init__.py#L19
    """
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
