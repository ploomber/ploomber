import warnings
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
    import jupyter_client
except ImportError:
    jupyter_client = None


from ploomber.exceptions import TaskBuildError, SourceInitializationError
from ploomber.sources import GenericSource
from ploomber.products import File, MetaProduct
from ploomber.tasks.Task import Task
from ploomber.util import requires


@requires(['jupytext'])
def _to_ipynb(source, extension, kernelspec_name=None):
    """Convert to jupyter notebook via jupytext

    Parameters
    ----------
    source : str
        Jupyter notebook (or jupytext compatible formatted) document

    extension : str
        Document format
    """
    nb = jupytext.reads(source, fmt={'extension': extension})

    has_parameters_tag = False

    for c in nb.cells:
        cell_tags = c.metadata.get('tags')

        if cell_tags:
            if 'parameters' in cell_tags:
                has_parameters_tag = True
                break

    if not has_parameters_tag:
        warnings.warn('Notebook does not have any cell with tag "parameters"'
                      'which is required by papermill')

    if nb.metadata.get('kernelspec') is None and kernelspec_name is None:
        raise ValueError('juptext could not load kernelspec from file and '
                         'kernelspec_name was not specified, either add '
                         'kernelspec info to your source file or specify '
                         'a kernelspec by name')

    if kernelspec_name is not None:
        k = jupyter_client.kernelspec.get_kernel_spec(kernelspec_name)

        nb.metadata.kernelspec = {
            "display_name": k.display_name,
            "language": k.language,
            "name": kernelspec_name
        }

    out = mktemp('.ipynb')
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
    """
    Run a Jupyter notebook using papermill. Support several input formats
    via jupytext and several output formats via nbconvert

    Parameters
    ----------
    source: str or pathlib.Path
        Notebook source, if str, the content is interpreted as the actual
        notebook, if pathlib.Path, the content of the file is loaded. When
        loading from a str, ext_in must be passed
    product: ploomber.File
        The output file
    dag: ploomber.DAG
        A DAG to add this task to
    name: str, optional
        A str to indentify this task. Should not already exist in the dag
    params: dict, optional
        Notebook parameters. This are passed as the "parameters" argument
        to the papermill.execute_notebook function, by default, "product"
        and "upstream" are included
    kernelspec_name: str, optional
        Kernelspec name to use, if the notebook already includes kernelspec
        data (in metadata.kernelspec), this is ignored, otherwise, the kernel
        is looked up using the jupyter_client.kernelspec.get_kernel_spec
        function
    nbconvert_exporter_name: str, optional
        Once the notebook is run, this parameter controls whether to export
        the notebook to a different parameter using the nbconvert package,
        it is not needed unless the extension cannot be used to infer the
        final output format, in which case the nbconvert.get_exporter is used
    ext_in: str, optional
        The input extension format. If source is a pathlib.Path, the extension
        from there is used, if loaded from a str, this parameter is needed
    nb_product_key: str, optional
        If the notebook is expected to generate other products, pass the key
        to identify the output notebook (i.e. if product is a list with 3
        ploomber.File, pass the index pointing to the notebook path). If the
        only output is the notebook itself, this parameter is not needed
    """
    PRODUCT_CLASSES_ALLOWED = (File, )

    @requires(['jupyter', 'papermill'], 'NotebookRunner')
    def __init__(self, source, product, dag, name=None, params=None,
                 papermill_params=None, kernelspec_name=None,
                 nbconvert_exporter_name=None, ext_in=None,
                 nb_product_key=None):
        self.papermill_params = papermill_params or {}
        self.kernelspec_name = kernelspec_name
        self.nbconvert_exporter_name = nbconvert_exporter_name
        self.ext_in = ext_in
        self.nb_product_key = nb_product_key
        super().__init__(source, product, dag, name, params)

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
            path_to_out = Path(str(self.product[self.nb_product_key]))
        else:
            path_to_out = Path(str(self.product))

        source = str(self.source)

        if self.source.loc is None:
            if self.ext_in is None:
                raise ValueError('If the source was loaded from a string '
                                 'you need to pass the ext_in parameter')

            ext_in = '.'+self.ext_in
        else:
            ext_in = Path(self.source.loc).suffix

        ext_out = path_to_out.suffix
        # we will run the notebook with this extension, regardless of the
        # user's choice, if any error happens, this will allow them to debug
        # we will change the extension after the notebook runs successfully
        path_to_out_nb = path_to_out.with_suffix('.ipynb')

        # need to convert to ipynb using jupytext
        if ext_in != '.ipynb':
            path_to_in = _to_ipynb(source, ext_in, self.kernelspec_name)
        else:
            # otherwise just save rendered code in a tmp file
            path_to_in = mktemp('.ipynb')
            Path(path_to_in).write_text(source)

        # papermill only allows JSON serializable parameters
        params = self.params.to_dict()
        params['product'] = params['product'].to_json_serializable()

        if params.get('upstream'):
            params['upstream'] = {k: n.to_json_serializable() for k, n
                                  in params['upstream'].items()}

        try:
            pm.execute_notebook(path_to_in, str(path_to_out_nb),
                                parameters=params,
                                **self.papermill_params)
        except Exception as e:
            raise TaskBuildError('An error ocurred when calling'
                                 ' papermil.execute_notebook') from e

        # if output format other than ipynb, convert using nbconvert
        # and overwrite
        if ext_out != '.ipynb' or self.nbconvert_exporter_name is not None:
            path_to_out_nb.rename(path_to_out)
            _from_ipynb(path_to_out, ext_out, self.nbconvert_exporter_name)
