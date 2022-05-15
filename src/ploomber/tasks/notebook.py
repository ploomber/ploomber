import os
import shlex
import pdb
import tempfile
import subprocess
from subprocess import PIPE
from pathlib import Path
from importlib.util import find_spec
import warnings

import jupytext
import nbformat
import nbconvert
from nbconvert import ExporterNameError

# this was introduced in nbconvert 6.0
try:
    from nbconvert.exporters.webpdf import WebPDFExporter
except ModuleNotFoundError:
    WebPDFExporter = None

# papermill is importing a deprecated module from pyarrow
with warnings.catch_warnings():
    warnings.simplefilter('ignore', FutureWarning)
    import papermill as pm

try:
    # this is an optional dependency
    from pyppeteer import chromium_downloader
except ImportError:
    chromium_downloader = None

from ploomber.exceptions import TaskBuildError, TaskInitializationError
from ploomber.sources import NotebookSource
from ploomber.sources.notebooksource import _cleanup_rendered_nb
from ploomber.products import File, MetaProduct
from ploomber.tasks.abc import Task
from ploomber.util import requires, chdir_code
from ploomber.io import FileLoaderMixin, pretty_print
from ploomber.util._sys import _python_bin


# TODO: ensure that all places where we call this function are unit tested
def _write_text_utf_8(path, text):
    """Write text using UTF-8 encoding

    Notes
    -----
    In some systems, UTF-8 is not the default encoding (this happens on some
    Windows configurations, which have CP-1252 as default). This can break
    storing some files if the str to write contains characters that do not
    exist in the default encoding
    (https://github.com/ploomber/ploomber/issues/334)

    Under normal circumstances, it's better to use the default encoding since
    it provides better system compatibility, however, notebooks are always
    stored as UTF-8 so it's ok to store them as such.

    nbconvert always stores as UTF-8:
    https://github.com/jupyter/nbconvert/blob/5d57c16b655602f4705f9147f671f2cbaadaebca/nbconvert/writers/files.py#L126

    nbformat always reads as UTF-8:
    https://github.com/jupyter/nbformat/blob/df63593b64a15ee1c37b522973c39e8674f93c5b/nbformat/__init__.py#L125
    """
    Path(path).write_text(text, encoding='utf-8')


def _suggest_passing_product_dictionary():
    if Path('pipeline.yaml').is_file():
        return """

If you want to generate multiple products, pass a dictionary to \
product in your pipeline.yaml:

product:
  # nb must contain the path to the output notebook
  nb: products/output-notebook.ipynb
  # other outputs (optional)
  data: products/some-data.csv
  more-data: products/more-data.csv
"""
    else:
        return ('\nIf you want this task '
                'to generate multiple products, pass a dictionary '
                'to "product", with the path to the '
                'output notebook in the "nb" key (e.g. "output.ipynb") '
                'and any other output paths in other keys)')


def _check_exporter(exporter, path_to_output):
    """
    Validate if the user can use the selected exporter
    """
    if WebPDFExporter is not None and exporter is WebPDFExporter:
        pyppeteer_installed = find_spec('pyppeteer') is not None

        if not pyppeteer_installed:
            raise TaskInitializationError(
                'pyppeteer is required to use '
                'webpdf, install it '
                'with:\npip install "nbconvert[webpdf]"')
        else:
            if not chromium_downloader.check_chromium():
                chromium_downloader.download_chromium()

        if Path(path_to_output).suffix != '.pdf':
            raise TaskInitializationError(
                'Expected output to have '
                'extension .pdf when using the webpdf '
                f'exporter, got: {path_to_output}. Change the extension '
                'and try again')


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
    # mapping of extensions to the corresponding nbconvert exporter
    EXTENSION2EXPORTER = {
        '.md': 'markdown',
        '.html': 'html',
        '.tex': 'latex',
        '.pdf': 'pdf',
        '.rst': 'rst',
        '.ipynb': 'ipynb',
    }

    def __init__(self,
                 path_to_output,
                 exporter_name=None,
                 nbconvert_export_kwargs=None):
        self._suffix = Path(path_to_output).suffix

        if exporter_name is None:

            # try to infer exporter name from the extension
            if not self._suffix:
                raise TaskInitializationError(
                    'Could not determine format for product '
                    f'{pretty_print.try_relative_path(path_to_output)!r} '
                    'because it has no extension. '
                    'Add a valid one '
                    f'({pretty_print.iterable(self.EXTENSION2EXPORTER)}) '
                    'or pass "nbconvert_exporter_name".' +
                    _suggest_passing_product_dictionary())

            if self._suffix in self.EXTENSION2EXPORTER:
                exporter_name = self.EXTENSION2EXPORTER[self._suffix]
            else:
                raise TaskInitializationError(
                    'Could not determine '
                    'format for product '
                    f'{pretty_print.try_relative_path(path_to_output)!r}. '
                    'Pass a valid extension '
                    f'({pretty_print.iterable(self.EXTENSION2EXPORTER)}) or '
                    'pass "nbconvert_exporter_name".' +
                    _suggest_passing_product_dictionary())

        self._exporter = self._get_exporter(exporter_name, path_to_output)

        self._path_to_output = path_to_output
        self._nbconvert_export_kwargs = nbconvert_export_kwargs or {}

    def convert(self):
        if self._exporter is None and self._nbconvert_export_kwargs:
            warnings.warn(
                f'Output {self._path_to_output!r} is a '
                'notebook file. nbconvert_export_kwargs '
                f'{self._nbconvert_export_kwargs!r} will be '
                'ignored since they only apply '
                'when exporting the notebook to other formats '
                'such as html. You may change the extension to apply '
                'the conversion parameters')

        if self._exporter is not None:
            self._from_ipynb(self._path_to_output, self._exporter,
                             self._nbconvert_export_kwargs)

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
                error = True
            else:
                error = False

            if error:
                names = nbconvert.get_export_names()
                raise TaskInitializationError(
                    f"{exporter_name!r} is not a "
                    "valid 'nbconvert_exporter_name' value. "
                    "Choose one from: "
                    f'{pretty_print.iterable(names)}')

        _check_exporter(exporter, path_to_output)

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
            _write_text_utf_8(path, content)
        elif isinstance(content, bytes):
            path.write_bytes(content)
        else:
            raise TypeError('nbconvert returned a converted notebook with'
                            'unknown format, only text and binary objects '
                            'are supported')

        return content


class NotebookMixin(FileLoaderMixin):
    # expose the static_analysis attribute from the source, we need this
    # since we disable static_analysis when rendering the DAG in Jupyter

    @property
    def static_analysis(self):
        return self._source.static_analysis

    @static_analysis.setter
    def static_analysis(self, value):
        self._source.static_analysis = value

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
        _write_text_utf_8(tmp, content)

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
        _write_text_utf_8(tmp_path, code)

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


class NotebookRunner(NotebookMixin, Task):
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
    static_analysis : ('disabled', 'regular', 'strict'), default='regular'
        Check for various errors in the notebook. In 'regular' mode, it aborts
        execution if the notebook has syntax issues, or similar problems that
        would cause the code to break if executed. In 'strict' mode, it
        performs the same checks but raises an isse before starting execution
        of any task, furthermore, it verifies that the parameters cell and
        the params passed to the notebook match, thus, making the notebook
        behave like a function with a signature.
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

    Spec API:

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
          - source: nb.ipynb
            product: report.html

    Spec API (multiple outputs):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
          - source: nb.ipynb
            product:
                # generated automatically by ploomber
                nb: report.html
                # must be generated by nb.ipynb
                data: data.csv

    Python API:

    >>> from pathlib import Path
    >>> from ploomber import DAG
    >>> from ploomber.tasks import NotebookRunner
    >>> from ploomber.products import File
    >>> dag = DAG()
    >>> NotebookRunner(Path('nb.ipynb'), File('report.html'), dag=dag)
    NotebookRunner: nb -> File('report.html')
    >>> dag.build() # doctest: +SKIP

    Python API (customize output notebook):

    >>> from pathlib import Path
    >>> from ploomber import DAG
    >>> from ploomber.tasks import NotebookRunner
    >>> from ploomber.products import File
    >>> dag = DAG()
    >>> # do not include input code (only cell's output)
    >>> NotebookRunner(Path('nb.ipynb'), File('out-1.html'), dag=dag,
    ...                nbconvert_export_kwargs={'exclude_input': True},
    ...                name='one')
    NotebookRunner: one -> File('out-1.html')
    >>> # Selectively remove cells with the tag "remove"
    >>> config = {'TagRemovePreprocessor': {'remove_cell_tags': ('remove',)},
    ...        'HTMLExporter':
    ...         {'preprocessors':
    ...    ['nbconvert.preprocessors.TagRemovePreprocessor']}}
    >>> NotebookRunner(Path('nb.ipynb'), File('out-2.html'), dag=dag,
    ...                nbconvert_export_kwargs={'config': config},
    ...                name='another')
    NotebookRunner: another -> File('out-2.html')
    >>> dag.build() # doctest: +SKIP

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
                 static_analysis='regular',
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
                raise TaskInitializationError(
                    f"Missing key '{nb_product_key}' in "
                    f"product: {(str(self.product))!r}. "
                    f"{nb_product_key!r} must contain "
                    "the path to the output notebook.")

        if isinstance(self.product, MetaProduct):
            product_nb = (
                self.product[self.nb_product_key]._identifier.best_repr(
                    shorten=False))
        else:
            product_nb = self.product._identifier.best_repr(shorten=False)

        self._converter = NotebookConverter(product_nb,
                                            nbconvert_exporter_name,
                                            nbconvert_export_kwargs)

    @staticmethod
    def _init_source(source,
                     kwargs,
                     ext_in=None,
                     kernelspec_name=None,
                     static_analysis='regular',
                     check_if_kernel_installed=False):
        return NotebookSource(
            source,
            ext_in=ext_in,
            kernelspec_name=kernelspec_name,
            static_analysis=static_analysis,
            check_if_kernel_installed=check_if_kernel_installed,
            **kwargs)

    def run(self):
        # regular mode: raise but not check signature
        # strict mode: called at render time
        if self.static_analysis == 'regular':
            self.source._check_notebook(raise_=True, check_signature=False)

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
        _write_text_utf_8(tmp, self.source.nb_str_rendered)

        if self.local_execution:
            self.papermill_params['cwd'] = str(self.source.loc.parent)

        # create parent folders if they don't exist
        Path(path_to_out_ipynb).parent.mkdir(parents=True, exist_ok=True)

        try:
            # no need to pass parameters, they are already there
            pm.execute_notebook(str(tmp), str(path_to_out_ipynb),
                                **self.papermill_params)
        except Exception as e:
            raise TaskBuildError(
                'Error when executing task'
                f' {self.name!r}. Partially'
                f' executed notebook available at {str(path_to_out_ipynb)}'
            ) from e
        finally:
            tmp.unlink()

        # on windows, Path.rename will throw an error if the file exists
        if path_to_out_ipynb != path_to_out and path_to_out.is_file():
            path_to_out.unlink()

        path_to_out_ipynb.rename(path_to_out)
        self._converter.convert()


class ScriptRunner(NotebookMixin, Task):
    """
    Similar to NotebookRunner, except it uses python to run the code,
    instead of papermill, hence, it doesn't generate an output notebook. But
    it also works by injecting a cell into the source code. Source can be
    a ``.py`` script or an ``.ipynb`` notebook. **Does not support magics.**

    Examples
    --------

    Spec API:

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
          - source: script.py
            class: ScriptRunner
            product:
                data: data.csv
                another: another.csv

    Python API:

    >>> from pathlib import Path
    >>> from ploomber import DAG
    >>> from ploomber.tasks import ScriptRunner
    >>> from ploomber.products import File
    >>> dag = DAG()
    >>> product = {'data': File('data.csv'), 'another': File('another.csv')}
    >>> _ = ScriptRunner(Path('script.py'), product, dag=dag)
    >>> _ = dag.build()
    """
    @requires(['jupyter', 'jupytext'], 'ScriptRunner')
    def __init__(self,
                 source,
                 product,
                 dag,
                 name=None,
                 params=None,
                 ext_in=None,
                 static_analysis='regular',
                 local_execution=False):
        self.ext_in = ext_in
        self.local_execution = local_execution

        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = ScriptRunner._init_source(source, kwargs, ext_in,
                                                 static_analysis)
        super().__init__(product, dag, name, params)

    @staticmethod
    def _init_source(source, kwargs, ext_in=None, static_analysis='regular'):
        return NotebookSource(source,
                              ext_in=ext_in,
                              static_analysis=static_analysis,
                              kernelspec_name=None,
                              check_if_kernel_installed=False,
                              **kwargs)

    def run(self):
        # regular mode: raise but not check signature
        # strict mode: called at render time
        if self.static_analysis == 'regular':
            self.source._check_notebook(raise_=True, check_signature=False)

        if self.local_execution:
            cwd = str(self.source.loc.parent)
        else:
            cwd = None

        fd, tmp = tempfile.mkstemp('.py')
        os.close(fd)

        code = '\n\n'.join([
            c['source'] for c in self.source.nb_obj_rendered.cells
            if c['cell_type'] == 'code'
        ])

        tmp = Path(tmp)
        tmp.write_text(code)

        if self.source.language == 'python':
            interpreter = _python_bin()
        elif self.source.language == 'r':
            interpreter = 'Rscript'
        else:
            raise ValueError(
                'ScriptRunner only works with Python and R scripts')

        try:
            _run_script_in_subprocess(interpreter, tmp, cwd)
        except Exception as e:
            raise TaskBuildError('Error when executing task'
                                 f' {self.name!r}.') from e
        finally:
            tmp.unlink()


def _run_script_in_subprocess(interpreter, path, cwd):
    res = subprocess.run([interpreter, str(path)], cwd=cwd, stderr=PIPE)

    if res.returncode:
        stderr = res.stderr.decode()

        if 'SyntaxError' in stderr:
            stderr += ('(Note: IPython magics are not supported in '
                       'ScriptRunner, remove them or use the regular '
                       'NotebookRunner)')

        raise RuntimeError('Error while executing ScriptRunner:\n' f'{stderr}')


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
