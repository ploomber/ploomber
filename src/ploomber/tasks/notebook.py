import os
import shlex
import pdb
import tempfile
import subprocess
from pathlib import Path
from nbconvert import ExporterNameError
import warnings

try:
    # papermill is importing a deprecated module from pyarrow
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', FutureWarning)
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
from ploomber.sources.notebooksource import _cleanup_rendered_nb
from ploomber.products import File, MetaProduct
from ploomber.tasks.abc import Task
from ploomber.util import requires, chdir_code
from ploomber.io import FileLoaderMixin


class NotebookConverter:
    """
    Thin wrapper around nbconvert to provide a simple API to convert .ipynb
    files to different formats.

    Parameters
    ----------
    nbconvert_export_kwargs : dict, default=None
        Keyword arguments passed to ``nbconvert.export``

    Notes
    -----
    The exporter is searched at initialization time to raise an appropriate
    error during Task setup rather than raising it at Task runtime

    Handles cases where the output representation is text (e.g. HTML) or bytes
    (e.g. PDF)
    """
    def __init__(self,
                 path_to_output,
                 exporter_name=None,
                 nbconvert_export_kwargs=None):
        if exporter_name is None:
            #  try to infer it from the extension
            suffix = Path(path_to_output).suffix

            if not suffix:
                raise ValueError('Could not determine output format for '
                                 'product: "{}" because it has no extension. '
                                 'Either add an extension '
                                 'or explicitly pass a '
                                 '"nbconvert_exporter_name" to the task '
                                 'constructor'.format(path_to_output))

            exporter_name = suffix[1:]

        self.exporter = self._get_exporter(exporter_name, path_to_output)
        self.path_to_output = path_to_output
        self.nbconvert_export_kwargs = nbconvert_export_kwargs or {}

    def convert(self):
        if self.exporter is None and self.nbconvert_export_kwargs:
            warnings.warn(
                f'Output {self.path_to_output!r} is a '
                'notebook file. nbconvert_export_kwargs '
                f'{self.nbconvert_export_kwargs!r} will be '
                'ignored since they only apply '
                'when exporting the notebook to other formats '
                'such as html. You may change the extension to apply '
                'the conversion parameters')

        if self.exporter is not None:
            self._from_ipynb(self.path_to_output, self.exporter,
                             self.nbconvert_export_kwargs)

    @staticmethod
    def _get_exporter(exporter_name, path_to_output):
        """
        Get function to convert notebook to another format using nbconvert,
        first. In some cases, the exporter name matches the file extension
        (e.g html) but other times it doesn't (e.g. slides), use
        `nbconvert.get_export_names()` to get available exporter_names

        Returns None if passed exported name is 'ipynb', raises ValueError
        if an exporter can't be located
        """
        extension2exporter_name = {'md': 'markdown'}

        # sometimes extension does not match with the exporter name, fix
        # if needed
        if exporter_name in extension2exporter_name:
            exporter_name = extension2exporter_name[exporter_name]

        if exporter_name == 'ipynb':
            exporter = None
        else:
            try:
                exporter = nbconvert.get_exporter(exporter_name)
            # nbconvert 5.6.1 raises ValueError, beginning in version 6,
            # it raises ExporterNameError. However the exception is defined
            # since 5.6.1 so we can safely import it
            except (ValueError, ExporterNameError):
                example_dict = {
                    'source': 'script.ipynb',
                    'product': {
                        'nb': 'output.ipynb',
                        'other': path_to_output
                    }
                }
                raise ValueError(
                    'Could not find nbconvert exporter '
                    'with name "{}". '
                    'Change the extension '
                    'or pass a valid "nbconvert_exporter_name" '
                    'value. Valid exporters are: {}.\n\nIf "{}" '
                    'is not intended to be the output noteboook, '
                    'register multiple products and identify the '
                    'output notebooks with "nb". Example: {}'.format(
                        exporter_name,
                        nbconvert.get_export_names(),
                        path_to_output,
                        example_dict,
                    ))

        return exporter

    @staticmethod
    def _from_ipynb(path_to_nb, exporter, nbconvert_export_kwargs):
        """
        Convert ipynb to another format
        """

        path = Path(path_to_nb)

        nb = nbformat.reads(path.read_text(), as_version=nbformat.NO_CONVERT)
        content, _ = nbconvert.export(exporter, nb, **nbconvert_export_kwargs)

        if isinstance(content, str):
            path.write_text(content)
        elif isinstance(content, bytes):
            path.write_bytes(content)
        else:
            raise TypeError('nbconvert returned a converted notebook with'
                            'unknown format, only text and binary objects '
                            'are supported')

        return content


class NotebookRunner(FileLoaderMixin, Task):
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
        Kernelspec name to use, if the file extension provides with enough
        information to choose a kernel or the notebook already includes
        kernelspec data (in metadata.kernelspec), this is ignored, otherwise,
        the kernel is looked up using jupyter_client.kernelspec.get_kernel_spec
    nbconvert_exporter_name: str, optional
        Once the notebook is run, this parameter controls whether to export
        the notebook to a different parameter using the nbconvert package,
        it is not needed unless the extension cannot be used to infer the
        final output format, in which case the nbconvert.get_exporter is used.
    ext_in: str, optional
        Source extension. Required if loading from a str. If source is a
        ``pathlib.Path``, the extension from the file is used.
    nb_product_key: str, optional
        If the notebook is expected to generate other products, pass the key
        to identify the output notebook (i.e. if product is a list with 3
        ploomber.File, pass the index pointing to the notebook path). If the
        only output is the notebook itself, this parameter is not needed
    static_analysis : bool
        Run static analysis after rendering. This compares the "params"
        argument with the declared arguments in the "parameters" cell (they
        make notebooks behave more like "functions"), pyflakes is also run to
        detect errors before executing the notebook. Has no effect if it's not
        a Python file.
    nbconvert_export_kwargs : dict
        Keyword arguments to pass to the ``nbconvert.export`` function (this is
        only used if exporting the output ipynb notebook to another format).
        You can use this, for example, to hide code cells using the
        exclude_input parameter. See ``nbconvert`` documentation for details.
        Ignored if the product is file with .ipynb extension.
    local_execution : bool, optional
        Change working directory to be the parent of the notebook's source.
        Defaults to False. This resembles the default behavior when
        running notebooks interactively via `jupyter notebook`


    Examples
    --------
    >>> from pathlib import Path
    >>> from ploomber import DAG
    >>> from ploomber.tasks import NotebookRunner
    >>> from ploomber.products import File
    >>> dag = DAG()
    >>> # do not include input code (only cell's output)
    >>> NotebookRunner(Path('nb.ipynb'), File('out-1.html'), dag=dag,
    ...                nbconvert_export_kwargs={'exclude_input': True},
    ...                name=1)
    >>> # Selectively remove cells with the tag "remove"
    >>> config = {'TagRemovePreprocessor': {'remove_cell_tags': ('remove',)},
    ...           'HTMLExporter':
    ...             {'preprocessors':
    ...             ['nbconvert.preprocessors.TagRemovePreprocessor']}}
    >>> NotebookRunner(Path('nb.ipynb'), File('out-2.html'), dag=dag,
    ...                nbconvert_export_kwargs={'config': config},
    ...                name=2)
    >>> dag.build()

    Notes
    -----
    nbconvert's documentation:
    https://nbconvert.readthedocs.io/en/latest/config_options.html#preprocessor-options
    """
    PRODUCT_CLASSES_ALLOWED = (File, )

    @requires(['jupyter', 'papermill', 'jupytext'], 'NotebookRunner')
    def __init__(self,
                 source,
                 product,
                 dag,
                 name=None,
                 params=None,
                 papermill_params=None,
                 kernelspec_name=None,
                 nbconvert_exporter_name=None,
                 ext_in=None,
                 nb_product_key='nb',
                 static_analysis=True,
                 nbconvert_export_kwargs=None,
                 local_execution=False,
                 check_if_kernel_installed=True):
        self.papermill_params = papermill_params or {}
        self.nbconvert_export_kwargs = nbconvert_export_kwargs or {}
        self.kernelspec_name = kernelspec_name
        self.nbconvert_exporter_name = nbconvert_exporter_name
        self.ext_in = ext_in
        self.nb_product_key = nb_product_key
        self.local_execution = local_execution
        self.check_if_kernel_installed = check_if_kernel_installed

        if 'cwd' in self.papermill_params and self.local_execution:
            raise KeyError('If local_execution is set to True, "cwd" should '
                           'not appear in papermill_params, as such '
                           'parameter will be set by the task itself')

        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = NotebookRunner._init_source(source, kwargs, ext_in,
                                                   kernelspec_name,
                                                   static_analysis,
                                                   check_if_kernel_installed)
        super().__init__(product, dag, name, params)

        if isinstance(self.product, MetaProduct):
            if self.product.get(nb_product_key) is None:
                raise KeyError("Key '{}' does not exist in product: {}. "
                               "Either add it to specify the output notebook "
                               "location or pass a 'nb_product_key' to the "
                               "task constructor with the key that contains "
                               " the output location".format(
                                   nb_product_key, str(self.product)))

        if isinstance(self.product, MetaProduct):
            product_nb = (
                self.product[self.nb_product_key]._identifier.best_repr(
                    shorten=False))
        else:
            product_nb = self.product._identifier.best_repr(shorten=False)

        self._converter = NotebookConverter(product_nb,
                                            nbconvert_exporter_name,
                                            nbconvert_export_kwargs)

    # expose the static_analysis attribute from the source, we need this
    # since we disable static_analysis when rendering the DAG in Jupyter

    @property
    def static_analysis(self):
        return self._source.static_analysis

    @static_analysis.setter
    def static_analysis(self, value):
        self._source.static_analysis = value

    @staticmethod
    def _init_source(source,
                     kwargs,
                     ext_in=None,
                     kernelspec_name=None,
                     static_analysis=False,
                     check_if_kernel_installed=False):
        return NotebookSource(
            source,
            ext_in=ext_in,
            kernelspec_name=kernelspec_name,
            static_analysis=static_analysis,
            check_if_kernel_installed=check_if_kernel_installed,
            **kwargs)

    def develop(self, app='notebook', args=None):
        """
        Opens the rendered notebook (with injected parameters) and adds a
        "debugging-settings" cell to the that changes directory to the current
        active directory. This will reflect conditions when callign
        `DAG.build()`. This modified notebook is saved in the same location as
        the source with a "-tmp" added to the filename. Changes to this
        notebook can be exported to the original notebook after the notebook
        process is shut down. The "injected-parameters" and
        "debugging-settings" cells are deleted before saving.

        Parameters
        ----------
        app : {'notebook', 'lab'}, default: 'notebook'
            Which Jupyter application to use
        args : str
            Extra parameters passed to the jupyter application

        Notes
        -----
        Be careful when developing tasks interacively. If the task has run
        successfully, you overwrite products but don't save the
        updated source code, your DAG will enter an inconsistent state where
        the metadata won't match the overwritten product.

        If you modify the source code and call develop again, the source
        code will be updated only if the ``hot_reload option`` is turned on.
        See :class:`ploomber.DAGConfigurator` for details.
        """
        # TODO: this code needs refactoring, should be a context manager
        # like the one we have for PythonCallable.develop that abstracts
        # the handling of the temporary notebook while editing

        apps = {'notebook', 'lab'}

        if app not in apps:
            raise ValueError('"app" must be one of {}, got: "{}"'.format(
                apps, app))

        if self.source.language != 'python':
            raise NotImplementedError(
                'develop is not implemented for "{}" '
                'notebooks, only python is supported'.format(
                    self.source.language))

        if self.source.loc is None:
            raise ValueError('Can only use develop in notebooks loaded '
                             'from files, not from str')

        nb = _read_rendered_notebook(self.source.nb_str_rendered)

        name = self.source.loc.name
        suffix = self.source.loc.suffix
        name_new = name.replace(suffix, '-tmp.ipynb')
        tmp = self.source.loc.with_name(name_new)
        content = nbformat.writes(nb, version=nbformat.NO_CONVERT)
        tmp.write_text(content)

        # open notebook with injected debugging cell
        try:
            subprocess.run(['jupyter', app, str(tmp)] +
                           shlex.split(args or ''),
                           check=True)
        except KeyboardInterrupt:
            print(f'Jupyter {app} application closed...')

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

        Parameters
        ----------
        kind : str, default='ipdb'
            Debugger to use, 'ipdb' to use line-by-line IPython debugger,
            'pdb' to use line-by-line Python debugger or 'pm' to to
            post-portem debugging using IPython

        Notes
        -----
        Be careful when debugging tasks. If the task has run
        successfully, you overwrite products but don't save the
        updated source code, your DAG will enter an inconsistent state where
        the metadata won't match the overwritten product.
        """
        if self.source.language != 'python':
            raise NotImplementedError(
                'debug is not implemented for "{}" '
                'notebooks, only python is supported'.format(
                    self.source.language))

        opts = {'ipdb', 'pdb', 'pm'}

        if kind not in opts:
            raise ValueError('kind must be one of {}'.format(opts))

        nb = _read_rendered_notebook(self.source.nb_str_rendered)
        fd, tmp_path = tempfile.mkstemp(suffix='.py')
        os.close(fd)
        code = jupytext.writes(nb, version=nbformat.NO_CONVERT, fmt='py')
        Path(tmp_path).write_text(code)

        if kind == 'pm':
            # post-mortem debugging
            try:
                subprocess.run(['ipython', tmp_path, '--pdb'])
            finally:
                Path(tmp_path).unlink()
        else:
            if kind == 'ipdb':
                from IPython.terminal.debugger import TerminalPdb, Pdb
                code = compile(source=code, filename=tmp_path, mode='exec')

                try:
                    # this seems to only work in a Terminal
                    debugger = TerminalPdb()
                except Exception:
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
        path_to_out_ipynb = path_to_out.with_suffix('.ipynb')

        fd, tmp = tempfile.mkstemp('.ipynb')
        os.close(fd)

        tmp = Path(tmp)
        tmp.write_text(self.source.nb_str_rendered)

        if self.local_execution:
            self.papermill_params['cwd'] = str(self.source.loc.parent)

        # create parent folders if they don't exist
        Path(path_to_out_ipynb).parent.mkdir(parents=True, exist_ok=True)

        try:
            # no need to pass parameters, they are already there
            pm.execute_notebook(str(tmp), str(path_to_out_ipynb),
                                **self.papermill_params)
        except Exception as e:
            raise TaskBuildError('An error occurred when calling'
                                 ' papermil.execute_notebook, partially'
                                 ' executed notebook with traceback '
                                 'available at {}'.format(
                                     str(path_to_out_ipynb))) from e
        finally:
            tmp.unlink()

        path_to_out_ipynb.rename(path_to_out)
        self._converter.convert()


def _read_rendered_notebook(nb_str):
    """
    Read rendered notebook and inject cell with debugging settings
    """
    # add debug cells
    nb = nbformat.reads(nb_str, as_version=nbformat.NO_CONVERT)
    nbformat_v = nbformat.versions[nb.nbformat]

    source = """
# Debugging settings (this cell will be removed before saving)
# change the current working directory to directory of the session that
# invoked the jupyter app to make relative paths work
import os
{}
""".format(chdir_code(Path('.').resolve()))

    cell = nbformat_v.new_code_cell(source,
                                    metadata={'tags': ['debugging-settings']})
    nb.cells.insert(0, cell)

    return nb


def _save():
    res = input('Notebook changed, do you want to save changes '
                'in the original location? (injected parameters '
                'and debugging cells will be removed before '
                'saving). Enter "no" to skip saving changes, '
                'anything else will be interpreted as "yes": ')
    return res != 'no'
