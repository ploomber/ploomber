from copy import copy
from tempfile import mktemp
from pathlib import Path

try:
    import papermill as pm
except ImportError:
    pm = None

try:
    import jupytext
except ImportError:
    jupytext = None

try:
    import nbformat
except ImportError:
    nbformat = None

try:
    import nbconvert
except ImportError:
    nbconvert = None

try:
    from jupyter_client import kernelspec
except ImportError:
    kernelspec = None


from ploomber.exceptions import TaskBuildError, SourceInitializationError
from ploomber.sources import GenericSource
from ploomber.products import File, MetaProduct
from ploomber.tasks.Task import Task


def _to_ipynb(source, extension, kernelspec_name=None):
    """Convert to jupyter notebook via jupytext
    """
    nb = jupytext.reads(source, fmt={'extension': extension})

    # tag the first cell as "parameters" for papermill to inject them
    nb.cells[0]['metadata']['tags'] = ["parameters"]

    if nb.metadata.get('kernelspec') is None and kernelspec_name is None:
        raise ValueError('juptext could not load kernelspec from file and '
                         'kernelspec_name was not specified, either add '
                         'kernelspec info to your source file or specify '
                         'a kernelspec by name')

    if kernelspec_name is not None:
        k = kernelspec.get_kernel_spec('python3')

        nb.metadata.kernelspec = {
            "display_name": k.display_name,
            "language": k.language,
            "name": "python3"
        }

    out = mktemp()
    Path(out).write_text(nbformat.v4.writes(nb))

    return out


def _from_ipynb(path_to_nb, extension, nbconvert_exporter_name):
    if nbconvert_exporter_name is not None:
        exporter = nbconvert.get_exporter(nbconvert_exporter_name)
    else:
        try:
            exporter = nbconvert.get_exporter(extension.replace('.', ''))
        except ValueError:
            raise TaskBuildError('Could not determine nbconvert exporter '
                                 'either specify in the path extension '
                                 'or pass a valid exporter name in '
                                 'the NotebookRunner constructor, '
                                 'valid expoers are: {}'
                                 .format(nbconvert.get_export_names()))

    path = Path(path_to_nb)

    nb = nbformat.v4.reads(path.read_text())
    content, _ = nbconvert.export(exporter, nb,
                                  exclude_input=True)

    path.write_text(content)

    return content


class NotebookRunner(Task):
    """Run a notebook using papermill
    """
    PRODUCT_CLASSES_ALLOWED = (File, )

    def __init__(self, source, product, dag, name=None, params=None,
                 papermill_params=None, kernelspec_name=None,
                 nbconvert_exporter_name=None, ext_in=None,
                 nb_product_key=None):
        self.papermill_params = papermill_params or {}
        self.kernelspec_name = kernelspec_name
        self.nbconvert_exporter_name = nbconvert_exporter_name
        self.ext_in = ext_in
        self.nb_product_key = nb_product_key
        super().__init__(source, product, dag, name, params or {})

        if isinstance(self.product, MetaProduct) and nb_product_key is None:
            raise KeyError('More than one product was passed but '
                           'nb_product_key '
                           'is None, pass a value to locate the notebook '
                           'save location')

    def _init_source(self, source):
        source = GenericSource(source)

        if source.needs_render:
            raise SourceInitializationError('The source for this task "{}"'
                                            ' must be a literal '
                                            .format(source.value.raw))

        return source

    def run(self):
        if isinstance(self.product, MetaProduct):
            path_to_out = str(self.product[self.nb_product_key])
        else:
            path_to_out = str(self.product)

        source = str(self.source)

        if self.source.loc is None:
            if self.ext_in is None:
                raise ValueError('If the source was loaded from a string '
                                 'you need to pass the ext_in parameter')

            ext_in = '.'+self.ext_in
        else:
            ext_in = Path(self.source.loc).suffix

        ext_out = Path(path_to_out).suffix

        # need to convert to ipynb using jupytext
        if ext_in != '.ipynb':
            path_to_in = _to_ipynb(source, ext_in, self.kernelspec_name)
        else:
            # otherwise just save rendered code in a tmp file
            path_to_in = mktemp()
            Path(path_to_in).write_text(source)

        # papermill only allows JSON serializable parameters
        params = copy(self.params)
        params['product'] = params['product']._to_json_serializable()

        if params.get('upstream'):
            params['upstream'] = {k: n._to_json_serializable() for k, n
                                  in params['upstream'].items()}

        pm.execute_notebook(path_to_in, path_to_out,
                            parameters=params,
                            **self.papermill_params)

        # if output format other than ipynb, convert using nbconvert
        # and overwrite
        if ext_out != '.ipynb' or self.nbconvert_exporter_name is not None:
            _from_ipynb(path_to_out, ext_out, self.nbconvert_exporter_name)
