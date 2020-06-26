import pdb
import tempfile
import subprocess
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


from ploomber.exceptions import TaskBuildError
from ploomber.sources import NotebookSource
from ploomber.sources.NotebookSource import _cleanup_rendered_nb
from ploomber.products import File, MetaProduct
from ploomber.tasks.Task import Task
from ploomber.util import requires


def _get_exporter(nbconvert_exporter_name, extension):
    if nbconvert_exporter_name is not None:
        exporter = nbconvert.get_exporter(nbconvert_exporter_name)
    else:
        if extension == '.ipynb':
            return None
        else:
            try:
                exporter = nbconvert.get_exporter(extension.replace('.', ''))
            except ValueError:
                raise ValueError('Could not determine nbconvert exporter '
                                 'either specify in the path extension '
                                 'or pass a valid exporter name in '
                                 'the NotebookRunner constructor, '
                                 'valid exporters are: {}'
                                 .format(nbconvert.get_export_names()))

    return exporter


def _from_ipynb(path_to_nb, exporter, nbconvert_export_kwargs):

    path = Path(path_to_nb)

    nb = nbformat.reads(path.read_text(), as_version=nbformat.NO_CONVERT)
    content, _ = nbconvert.export(exporter, nb,
                                  **nbconvert_export_kwargs)

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
    papermill_params : dict, optional
        Other parameters passed to papermill.execute_notebook, defaults to None
    kernelspec_name: str, optional
        Kernelspec name to use, if the notebook already includes kernelspec
        data (in metadata.kernelspec), this is ignored, otherwise, the kernel
        is looked up using the jupyter_client.kernelspec.get_kernel_spec
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
    static_analysis : bool
        Run static analysis after rendering.
        This requires a cell with the tag 'parameters' to exist in the
        notebook, such cell should have at least a "product = None" variable
        declared. Passed and declared parameters are compared (they make
        notebooks behave more like "functions"), pyflakes is also run to
        detect errors before executing the notebook. If the task has
        upstream dependencies an upstream parameter should also be declared
        "upstream = None"
    nbconvert_export_kwargs : dict
        Keyword arguments to pass to the nbconvert.export function (this is
        only used if exporting the output ipynb notebook to another format).
        You can use this for example to hide code cells using the exclude_input
        parameter
    """
    PRODUCT_CLASSES_ALLOWED = (File, )

    @requires(['jupyter', 'papermill', 'jupytext'], 'NotebookRunner')
    def __init__(self, source, product, dag, name=None, params=None,
                 papermill_params=None, kernelspec_name='python3',
                 nbconvert_exporter_name=None, ext_in=None,
                 nb_product_key='nb', static_analysis=False,
                 nbconvert_export_kwargs=None):
        self.papermill_params = papermill_params or {}
        self.nbconvert_export_kwargs = nbconvert_export_kwargs or {}
        self.kernelspec_name = kernelspec_name
        self.nbconvert_exporter_name = nbconvert_exporter_name
        self.ext_in = ext_in
        self.nb_product_key = nb_product_key
        self.static_analysis = static_analysis
        super().__init__(source, product, dag, name, params)

        if isinstance(self.product, MetaProduct):
            if self.product.get(nb_product_key) is None:
                raise KeyError('Key "{}" does not exist in product: {}. '
                               'nb_product_key should be an existing '
                               'key to know where to save the output '
                               'notebook'.format(nb_product_key,
                                                 str(self.product)))

        if isinstance(self.product, MetaProduct):
            ext_out = self.product[self.nb_product_key].suffix
        else:
            ext_out = self.product.suffix

        self._exporter = _get_exporter(nbconvert_exporter_name, ext_out)

    def _init_source(self, source, kwargs):
        return NotebookSource(source,
                              ext_in=self.ext_in,
                              kernelspec_name=self.kernelspec_name,
                              static_analysis=self.static_analysis,
                              **kwargs)

    def develop(self):
        """
        Opens the rendered notebook (with injected parameters) and adds a
        "debugging-settings" cell to the that changes directory to the current
        active directory. This will reflect conditions when callign
        `DAG.build()`. This modified notebook is saved in the same location as
        the source with a "-tmp" added to the filename. Changes to this
        notebook can be exported to the original notebook after the notebook
        process is shut down. The "injected-parameters" and
        "debugging-settings" cells are deleted before saving.

        Notes
        -----
        If you modify the source code and call develop again, the source
        code will be updated only if the `hot_reload option` is turned on.
        See :class:`ploomber.DAGConfigurator` for details.
        """
        if self.source.loc is None:
            raise ValueError('Can only use develop in notebooks loaded '
                             'from files, not from str')

        nb = _read_rendered_notebook(self.source.rendered_nb_str)

        name = self.source.loc.name
        suffix = self.source.loc.suffix
        name_new = name.replace(suffix, '-tmp.ipynb')
        tmp = self.source.loc.with_name(name_new)
        content = nbformat.writes(nb, version=nbformat.NO_CONVERT)
        tmp.write_text(content)

        # open notebook with injected debugging cell
        _open_jupyter_notebook(str(tmp))

        # read tmp file again, to see if the user made any changes
        content_new = Path(tmp).read_text()

        # maybe exclude changes in tmp cells?
        if content == content_new:
            print('No changes found...')
        else:
            # save changes
            if _save():
                nb = nbformat.reads(content_new,
                                    as_version=nbformat.NO_CONVERT)

                # remove injected-parameters and debugging-settings cells if
                # they exist
                _cleanup_rendered_nb(nb)

                # write back in the same format and original location
                ext_source = Path(self.source.loc).suffix[1:]
                print('Saving notebook to: ', self.source.loc)
                jupytext.write(nb, self.source.loc, fmt=ext_source)
            else:
                print('Not saving changes...')

        # remove tmp file
        Path(tmp).unlink()

    def debug(self, kind='ipdb'):
        """
        Opens the notebook (with injected parameters) in debug mode in a
        temporary location
        """
        opts = {'ipdb', 'pdb'}

        if kind not in opts:
            raise ValueError('kind must be one of {}'.format(opts))

        nb = _read_rendered_notebook(self.source.rendered_nb_str)
        _, tmp_path = tempfile.mkstemp(suffix='.ipynb')
        code = jupytext.writes(nb, version=nbformat.NO_CONVERT, fmt='py')
        Path(tmp_path).write_text(code)

        if kind == 'ipdb':
            from IPython.terminal.debugger import TerminalPdb, Pdb
            code = compile(source=code, filename=tmp_path, mode='exec')

            try:
                # this seems to only work in a Terminal
                debugger = TerminalPdb()
            except AttributeError:
                # this works in a Jupyter notebook
                debugger = Pdb()
        elif kind == 'pdb':
            debugger = pdb

        try:
            debugger.run(code)
        finally:
            Path(tmp_path).unlink()

    def run(self):
        if isinstance(self.product, MetaProduct):
            path_to_out = Path(str(self.product[self.nb_product_key]))
        else:
            path_to_out = Path(str(self.product))

        # we will run the notebook with this extension, regardless of the
        # user's choice, if any error happens, this will allow them to debug
        # we will change the extension after the notebook runs successfully
        path_to_out_nb = path_to_out.with_suffix('.ipynb')

        _, tmp = tempfile.mkstemp('.ipynb')
        tmp = Path(tmp)
        tmp.write_text(self.source.rendered_nb_str)

        try:
            # no need to pass parameters, they are already there
            pm.execute_notebook(str(tmp), str(path_to_out_nb),
                                **self.papermill_params)
        except Exception as e:
            raise TaskBuildError('An error ocurred when calling'
                                 ' papermil.execute_notebook, partially'
                                 ' executed notebook with traceback '
                                 'available at {}'
                                 .format(str(path_to_out_nb))) from e
        else:
            tmp.unlink()

        # if output format other than ipynb, convert using nbconvert
        # and overwrite
        if self._exporter is not None:
            path_to_out_nb.rename(path_to_out)
            _from_ipynb(path_to_out, self._exporter,
                        self.nbconvert_export_kwargs)


def _read_rendered_notebook(nb_str):
    """
    Read rendered notebook and inject cell with debugging settings
    """
    # add debug cells
    nb = nbformat.reads(nb_str, as_version=nbformat.NO_CONVERT)
    nbformat_v = nbformat.versions[nb.nbformat]

    source = """
# Debugging settings (this cell will be removed before saving)
# change the current working directory to the one when .debug() happen
# to make relative paths work
from os import chdir
chdir("{}")
""".format(Path('.').resolve())

    cell = nbformat_v.new_code_cell(source,
                                    metadata={'tags':
                                              ['debugging-settings']})
    nb.cells.insert(0, cell)

    return nb


def _open_jupyter_notebook(path):
    try:
        subprocess.call(['jupyter', 'notebook', path])
    except KeyboardInterrupt:
        print('Jupyter notebook server closed...')


def _save():
    res = input('Notebook changed, do you want to save changes '
                'in the original location? (injected parameters '
                'and debugging cells will be removed before '
                'saving). Enter "no" to skip saving changes, '
                'anything else will be interpreted as "yes": ')
    return res != 'no'
