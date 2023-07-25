import os
import pdb
import shutil
import tempfile
import subprocess
from subprocess import PIPE
from pathlib import Path, PurePosixPath
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
    warnings.simplefilter("ignore", FutureWarning)
    import papermill as pm

try:
    # this is an optional dependency
    from pyppeteer import chromium_downloader
except ImportError:
    chromium_downloader = None

from ploomber.exceptions import TaskBuildError, TaskInitializationError
from ploomber.sources import NotebookSource
from ploomber.products import File, MetaProduct
from ploomber.tasks.abc import Task
from ploomber.util import chdir_code
from ploomber.io import FileLoaderMixin, pretty_print, _validate
from ploomber.util._sys import _python_bin

import ploomber_engine as pe


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
    Path(path).write_text(text, encoding="utf-8")


def _suggest_passing_product_dictionary():
    if Path("pipeline.yaml").is_file():
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
        return (
            "\nIf you want this task "
            "to generate multiple products, pass a dictionary "
            'to "product", with the path to the '
            'output notebook in the "nb" key (e.g. "output.ipynb") '
            "and any other output paths in other keys)"
        )


def _check_exporter(exporter, path_to_output):
    """
    Validate if the user can use the selected exporter
    """
    if WebPDFExporter is not None and exporter is WebPDFExporter:
        pyppeteer_installed = find_spec("pyppeteer") is not None

        if not pyppeteer_installed:
            raise TaskInitializationError(
                "pyppeteer is required to use "
                "webpdf, install it "
                'with:\npip install "nbconvert[webpdf]"'
            )
        else:
            if not chromium_downloader.check_chromium():
                chromium_downloader.download_chromium()

        if Path(path_to_output).suffix != ".pdf":
            raise TaskInitializationError(
                "Expected output to have "
                "extension .pdf when using the webpdf "
                f"exporter, got: {path_to_output}. Change the extension "
                "and try again"
            )


def _safe_suffix(product):
    try:
        return Path(product).suffix
    except Exception:
        return None


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
        ".md": "markdown",
        ".html": "html",
        ".tex": "latex",
        ".pdf": "pdf",
        ".rst": "rst",
        ".ipynb": "ipynb",
    }

    def __init__(
        self, path_to_output, exporter_name=None, nbconvert_export_kwargs=None
    ):
        self._suffix = Path(path_to_output).suffix

        if exporter_name is None:
            # try to infer exporter name from the extension
            if not self._suffix:
                raise TaskInitializationError(
                    "Could not determine format for product "
                    f"{pretty_print.try_relative_path(path_to_output)!r} "
                    "because it has no extension. "
                    "Add a valid one "
                    f"({pretty_print.iterable(self.EXTENSION2EXPORTER)}) "
                    'or pass "nbconvert_exporter_name".'
                    + _suggest_passing_product_dictionary()
                )

            if self._suffix in self.EXTENSION2EXPORTER:
                exporter_name = self.EXTENSION2EXPORTER[self._suffix]
            else:
                raise TaskInitializationError(
                    "Could not determine "
                    "format for product "
                    f"{pretty_print.try_relative_path(path_to_output)!r}. "
                    "Pass a valid extension "
                    f"({pretty_print.iterable(self.EXTENSION2EXPORTER)}) or "
                    'pass "nbconvert_exporter_name".'
                    + _suggest_passing_product_dictionary()
                )

        self._exporter = self._get_exporter(exporter_name, path_to_output)

        self._path_to_output = path_to_output
        self._nbconvert_export_kwargs = nbconvert_export_kwargs or {}

    def convert(self):
        if self._exporter is None and self._nbconvert_export_kwargs:
            warnings.warn(
                f"Output {self._path_to_output!r} is a "
                "notebook file. nbconvert_export_kwargs "
                f"{self._nbconvert_export_kwargs!r} will be "
                "ignored since they only apply "
                "when exporting the notebook to other formats "
                "such as html. You may change the extension to apply "
                "the conversion parameters"
            )

        if self._exporter is not None:
            self._from_ipynb(
                self._path_to_output, self._exporter, self._nbconvert_export_kwargs
            )

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
        extension2exporter_name = {"md": "markdown"}

        # sometimes extension does not match with the exporter name, fix
        # if needed
        if exporter_name in extension2exporter_name:
            exporter_name = extension2exporter_name[exporter_name]

        if exporter_name == "ipynb":
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
                    f"{pretty_print.iterable(names)}"
                )

        _check_exporter(exporter, path_to_output)

        return exporter

    @staticmethod
    def _from_ipynb(path_to_nb, exporter, nbconvert_export_kwargs):
        """
        Convert ipynb to another format
        """

        path = Path(path_to_nb)

        nb = nbformat.reads(
            path.read_text(encoding="utf-8"), as_version=nbformat.NO_CONVERT
        )
        content, _ = nbconvert.export(exporter, nb, **nbconvert_export_kwargs)

        if isinstance(content, str):
            _write_text_utf_8(path, content)
        elif isinstance(content, bytes):
            path.write_bytes(content)
        else:
            raise TypeError(
                "nbconvert returned a converted notebook with"
                "unknown format, only text and binary objects "
                "are supported"
            )

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

    def debug(self, kind="ipdb"):
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
        if self.source.language != "python":
            raise NotImplementedError(
                'debug is not implemented for "{}" '
                "notebooks, only python is supported".format(self.source.language)
            )

        opts = {"ipdb", "pdb", "pm"}

        if kind not in opts:
            raise ValueError("kind must be one of {}".format(opts))

        nb = _read_rendered_notebook(self.source.nb_str_rendered)
        fd, tmp_path = tempfile.mkstemp(suffix=".py")
        os.close(fd)
        code = jupytext.writes(nb, version=nbformat.NO_CONVERT, fmt="py")
        _write_text_utf_8(tmp_path, code)

        if kind == "pm":
            # post-mortem debugging
            try:
                subprocess.run(["ipython", tmp_path, "--pdb"])
            finally:
                Path(tmp_path).unlink()
        else:
            if kind == "ipdb":
                from IPython.terminal.debugger import TerminalPdb, Pdb

                code = compile(source=code, filename=tmp_path, mode="exec")

                try:
                    # this seems to only work in a Terminal
                    debugger = TerminalPdb()
                except Exception:
                    # this works in a Jupyter notebook
                    debugger = Pdb()

            elif kind == "pdb":
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
    executor: str, optional
        executor to use. Currently supports "ploomber-engine" and "papermill".
        Defaults to papermill executor. Can also be passed as "engine_name"
        in executor_params
    executor_params: dict, optional
        Parameters passed to executor, defaults to None. Please refer to each
        executor execute_notebook APIs to learn more about this.
    papermill_params : dict, optional
        Other parameters passed to papermill.execute_notebook, defaults to None
    kernelspec_name: str, optional
        Kernelspec name to use, if the file extension provides with enough
        information to choose a kernel or the notebook already includes
        kernelspec data (in metadata.kernelspec), this is ignored, otherwise,
        the kernel is looked up using jupyter_client.kernelspec.get_kernel_spec
    nbconvert_exporter_name: str or dict, optional
        Once the notebook is run, this parameter controls whether to export
        the notebook to a different parameter using the nbconvert package,
        it is not needed unless the extension cannot be used to infer the
        final output format, in which case the nbconvert.get_exporter is used.
        If nb_product_key is a list of multiple nb products keys,
        nbconvert_exporter_name should be a dict containing keys from this
        list.
    ext_in: str, optional
        Source extension. Required if loading from a str. If source is a
        ``pathlib.Path``, the extension from the file is used.
    nb_product_key: str or list, optional
        If the notebook is expected to generate other products, pass the key
        to identify the output notebook (i.e. if product is a list with 3
        ploomber.File, pass the index pointing to the notebook path). If the
        only output is the notebook itself, this parameter is not needed
        If multiple notebook conversions are required like html, pdf, this
        parameter should be a list of keys like 'nb_ipynb', 'nb_html, 'nb_pdf'.
    static_analysis : ('disabled', 'regular', 'strict'), default='regular'
        Check for various errors in the notebook. In 'regular' mode, it aborts
        execution if the notebook has syntax issues, or similar problems that
        would cause the code to break if executed. In 'strict' mode, it
        performs the same checks but raises an issue before starting execution
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
    debug_mode : None, 'now'  or 'later', default=None
        If 'now', runs notebook in debug mode, this will start debugger if an
        error is thrown. If 'later', it will serialize the traceback for later
        debugging. (Added in 0.20)

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

    Spec API (multiple notebook products, added in ``0.19.6``):

    (generate the executed notebooks in multiple formats)

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
          - source: script.py
            # keys can be named as per user's choice. None
            # of the keys are mandatory. However, every key mentioned
            # in this list should be a part of the product dict below.
            nb_product_key: [nb_ipynb, nb_pdf, nb_html]
            # When nb_product_key is a list, nbconvert_exporter_name
            # should be a dict with required keys from nb_product_key
            # only. If missing, it uses the default exporter
            nbconvert_exporter_name:
                nb_pdf: webpdf
            # Every notebook product defined here should correspond to key
            # defined in nb_product_key.
            product:
                nb_ipynb: nb.ipynb
                nb_pdf: doc.pdf
                nb_html: report.html
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
    .. collapse:: changelog

        .. versionchanged:: 0.22.4
            Added native ploomber-engine support with `executor`
            parameter

        .. versionchanged:: 0.20
            ``debug`` constructor flag renamed to ``debug_mode`` to prevent
            conflicts with the ``debug`` method

        .. versionchanged:: 0.19.6
            Support for generating output notebooks in multiple formats, see
            example above.

    `nbconvert's documentation <https://nbconvert.readthedocs.io/en/latest/config_options.html#preprocessor-options>`_
    """  # noqa

    PRODUCT_CLASSES_ALLOWED = (File,)

    def _validate_nbconvert_exporter(self):
        if isinstance(self.nb_product_key, list) and isinstance(
            self.nbconvert_exporter_name, str
        ):
            raise TaskInitializationError(
                f"When specifying nb_product_key as a list, "
                f"create a dictionary under nbconvert_exporter_name "
                f"with required keys in {self.nb_product_key!r}. "
            )

        if isinstance(self.nb_product_key, str) and isinstance(
            self.nbconvert_exporter_name, dict
        ):
            raise TaskInitializationError(
                "Please specify a single nbconvert_exporter_name "
                "when generating a single notebook product. "
                "Example : 'nbconvert_exporter_name': '<exporter name'> "
            )

        if isinstance(self.nb_product_key, list) and isinstance(
            self.nbconvert_exporter_name, dict
        ):
            for exporter_name in self.nbconvert_exporter_name:
                if exporter_name not in self.nb_product_key:
                    raise TaskInitializationError(
                        f"Invalid nbconvert exporter : {exporter_name!r}. "
                        f"All exporter names in nbconvert_exporter_name "
                        f"should be present in nb_product_key"
                    )

    def _validate_nb_product_key(self):
        for key in self.nb_product_key:
            if self.product.get(key) is None:
                raise TaskInitializationError(
                    f"Missing key '{key}' in "
                    f"product: {(str(self.product))!r}. "
                    f"All keys specified in "
                    f"{self.nb_product_key!r} must contain the "
                    f"corresponding product path."
                )

        for key, path in self.product.to_json_serializable().items():
            if (
                _safe_suffix(path) in [".md", ".html", ".tex", ".pdf", ".rst", ".ipynb"]
                and key not in self.nb_product_key
            ):
                raise TaskInitializationError(
                    f"Missing key '{key}' in "
                    f"nb_product_key: {self.nb_product_key!r}. "
                    f"All notebook product keys specified in "
                    f"{(str(self.product))!r} must be specified "
                    f"in nb_product_key as well."
                )

    def __init__(
        self,
        source,
        product,
        dag,
        name=None,
        params=None,
        executor="papermill",
        executor_params=None,
        papermill_params=None,
        kernelspec_name=None,
        nbconvert_exporter_name=None,
        ext_in=None,
        nb_product_key="nb",
        static_analysis="regular",
        nbconvert_export_kwargs=None,
        local_execution=False,
        check_if_kernel_installed=True,
        debug_mode=None,
    ):
        self.executor = executor
        self.executor_params = executor_params or {}
        self.papermill_params = papermill_params or {}
        self.nbconvert_export_kwargs = nbconvert_export_kwargs or {}
        self.kernelspec_name = kernelspec_name
        self.nbconvert_exporter_name = nbconvert_exporter_name
        self.ext_in = ext_in
        self.nb_product_key = nb_product_key
        self.local_execution = local_execution
        self.check_if_kernel_installed = check_if_kernel_installed
        self.debug_mode = debug_mode

        if self.executor not in ["papermill", "ploomber-engine"]:
            raise ValueError(
                f"Invalid executor : {self.executor}. "
                f"Please choose from 'papermill' or 'ploomber-engine'"
            )

        # We are migrating to executor_params
        if self.papermill_params:
            if not self.executor_params:
                warnings.warn(
                    "papermill_params will be deprecated in future releases."
                    "Please rename the section to executor_params",
                    FutureWarning,
                )
                self.executor_params = self.papermill_params
            else:
                warnings.warn(
                    "Both papermill_params and executor_params passed. "
                    "Overriding with executor_params",
                    UserWarning,
                )

        if "engine_name" in self.executor_params and self.debug_mode:
            raise ValueError(
                '"engine_name" should not appear in "executor_params " '
                'when "debug_mode" is enabled'
            )

        if "engine_name" in self.executor_params:
            if self.executor != self.executor_params["engine_name"]:
                raise KeyError(
                    "Found conflicting options: executor is set "
                    f'to {self.executor} but "engine_name" is set to '
                    f'{self.executor_params["engine_name"]} in "executor_params '
                    "Please use only one of the parameters or "
                    "pass the same executor to both"
                )

        if "cwd" in self.executor_params and self.local_execution:
            raise KeyError(
                'If local_execution is set to True, "cwd" should '
                "not appear in executor_params, as such "
                "parameter will be set by the task itself"
            )

        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = NotebookRunner._init_source(
            source,
            kwargs,
            ext_in,
            kernelspec_name,
            static_analysis,
            check_if_kernel_installed,
            False,
            False,
        )

        if self.source.language == "r" and self.executor == "ploomber-engine":
            raise ValueError("Ploomber Engine currently does not support R notebooks")
        super().__init__(product, dag, name, params)

        self._validate_nbconvert_exporter()

        if isinstance(self.product, MetaProduct):
            if isinstance(self.nb_product_key, list):
                self._validate_nb_product_key()

            elif self.product.get(nb_product_key) is None:
                raise TaskInitializationError(
                    f"Missing key '{nb_product_key}' in "
                    f"product: {(str(self.product))!r}. "
                    f"{nb_product_key!r} must contain "
                    "the path to the output notebook."
                )

        if isinstance(self.nb_product_key, str):
            if isinstance(self.product, MetaProduct):
                product_nb = self.product[self.nb_product_key]._identifier.best_repr(
                    shorten=False
                )

            else:
                product_nb = self.product._identifier.best_repr(shorten=False)

        else:
            multiple_product_nb = {
                key: (self.product[key]._identifier.best_repr(shorten=False))
                for key in self.nb_product_key
            }

        if isinstance(self.nb_product_key, str):
            self._converter = NotebookConverter(
                product_nb, nbconvert_exporter_name, nbconvert_export_kwargs
            )
        else:
            self._converter = []
            for key, product_nb in multiple_product_nb.items():
                exporter = (
                    nbconvert_exporter_name.get(key)
                    if nbconvert_exporter_name
                    else None
                )
                self._converter.append(
                    NotebookConverter(product_nb, exporter, nbconvert_export_kwargs)
                )

    @property
    def debug_mode(self):
        return self._debug_mode

    @debug_mode.setter
    def debug_mode(self, value):
        _validate.is_in(value, {None, "now", "later"}, "debug_mode")
        self._debug_mode = value

    @staticmethod
    def _init_source(
        source,
        kwargs,
        ext_in=None,
        kernelspec_name=None,
        static_analysis="regular",
        check_if_kernel_installed=False,
        extract_up=False,
        extract_prod=False,
    ):
        ns = NotebookSource(
            source,
            ext_in=ext_in,
            kernelspec_name=kernelspec_name,
            static_analysis=static_analysis,
            check_if_kernel_installed=check_if_kernel_installed,
            **kwargs,
        )
        ns._validate_parameters_cell(extract_up, extract_prod)
        return ns

    def run(self):
        # regular mode: raise but not check signature
        # strict mode: called at render time
        if self.static_analysis == "regular":
            self.source._check_notebook(raise_=True, check_signature=False)

        multiple_nb_products = []

        # the task generates more than one product
        if isinstance(self.product, MetaProduct):
            # generating a single output notebook
            if isinstance(self.nb_product_key, str):
                nb_product = self.product[self.nb_product_key]

            # generates multiple output notebooks
            else:
                # grab the last one by default
                key_to_use = self.nb_product_key[-1]

                for key in self.nb_product_key:
                    path = Path(str(self.product[key]))
                    multiple_nb_products.append(path)

                    # but prefer the one which is already an ipynb file
                    if path.suffix == ".ipynb":
                        key_to_use = key

                nb_product = self.product[key_to_use]

        # only one product - it must be the output notebook
        else:
            nb_product = self.product

        # we will run the notebook with this extension, regardless of the
        # user's choice, if any error happens, this will allow them to debug
        # we will change the extension after the notebook runs successfully
        path_to_out = Path(str(nb_product))
        path_to_out_ipynb = path_to_out.with_suffix(".ipynb")

        fd, tmp = tempfile.mkstemp(".ipynb")
        os.close(fd)

        tmp = Path(tmp)
        _write_text_utf_8(tmp, self.source.nb_str_rendered)

        if self.local_execution:
            self.executor_params["cwd"] = str(self.source.loc.parent)

        # use our custom executor
        if self.debug_mode == "now":
            self.executor_params["engine_name"] = "debug"
        elif self.debug_mode == "later":
            self.executor_params["engine_name"] = "debuglater"
            self.executor_params["path_to_dump"] = f"{self.name}.dump"

        # create parent folders if they don't exist
        Path(path_to_out_ipynb).parent.mkdir(parents=True, exist_ok=True)

        try:
            # no need to pass parameters, they are already there
            if self.executor == "ploomber-engine":
                pe.execute_notebook(
                    str(tmp), str(path_to_out_ipynb), **self.executor_params
                )
            elif self.executor == "papermill":
                pm.execute_notebook(
                    str(tmp), str(path_to_out_ipynb), **self.executor_params
                )

        except Exception as e:
            # upload partially executed notebook if there's a clint
            if nb_product.client:
                nb_product.client.upload(str(path_to_out_ipynb))
                parent = nb_product.client.parent
                remote_path = str(PurePosixPath(parent, path_to_out_ipynb))

                raise TaskBuildError(
                    "Error when executing task"
                    f" {self.name!r}. Partially"
                    " executed notebook uploaded to "
                    "remote storage at: "
                    f"{remote_path}"
                ) from e
            else:
                raise TaskBuildError(
                    "Error when executing task"
                    f" {self.name!r}. Partially"
                    " executed notebook "
                    f"available at {str(path_to_out_ipynb)}",
                    self,
                ) from e
        finally:
            tmp.unlink()

        # on windows, Path.rename will throw an error if the file exists
        if path_to_out_ipynb != path_to_out and path_to_out.is_file():
            path_to_out.unlink()

        for product in multiple_nb_products:
            if path_to_out_ipynb != product:
                shutil.copyfile(path_to_out_ipynb, product)

        path_to_out_ipynb.rename(path_to_out)

        if isinstance(self._converter, list):
            for _converter in self._converter:
                _converter.convert()
        else:
            self._converter.convert()


class ScriptRunner(NotebookMixin, Task):
    """
    Similar to NotebookRunner, except it uses python to run the code,
    instead of papermill, hence, it doesn't generate an output notebook. But
    it also works by injecting a cell into the source code. Source can be
    a ``.py`` script or an ``.ipynb`` notebook. **Does not support magics.**

    Parameters
    ----------
    source: str or pathlib.Path
        Script source, if str, the content is interpreted as the actual
        script, if pathlib.Path, the content of the file is loaded. When
        loading from a str, ext_in must be passed
    product: ploomber.File
        The output file
    dag: ploomber.DAG
        A DAG to add this task to
    name: str, optional
        A str to indentify this task. Should not already exist in the dag
    params: dict, optional
        Script parameters. This are passed as the "parameters" argument
        to the papermill.execute_notebook function, by default, "product"
        and "upstream" are included
    ext_in: str, optional
        Source extension. Required if loading from a str. If source is a
        ``pathlib.Path``, the extension from the file is used.
    static_analysis : ('disabled', 'regular', 'strict'), default='regular'
        Check for various errors in the script. In 'regular' mode, it aborts
        execution if the notebook has syntax issues, or similar problems that
        would cause the code to break if executed. In 'strict' mode, it
        performs the same checks but raises an issue before starting execution
        of any task, furthermore, it verifies that the parameters cell and
        the params passed to the notebook match, thus, making the script
        behave like a function with a signature.
    local_execution : bool, optional
        Change working directory to be the parent of the script source.
        Defaults to False.

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

    def __init__(
        self,
        source,
        product,
        dag,
        name=None,
        params=None,
        ext_in=None,
        static_analysis="regular",
        local_execution=False,
    ):
        self.ext_in = ext_in

        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = ScriptRunner._init_source(
            source, kwargs, ext_in, static_analysis, False, False
        )
        self.local_execution = local_execution
        super().__init__(product, dag, name, params)

    @staticmethod
    def _init_source(
        source,
        kwargs,
        ext_in=None,
        static_analysis="regular",
        extract_up=False,
        extract_prod=False,
    ):
        ns = NotebookSource(
            source,
            ext_in=ext_in,
            static_analysis=static_analysis,
            kernelspec_name=None,
            check_if_kernel_installed=False,
            **kwargs,
        )
        ns._validate_parameters_cell(extract_up, extract_prod)
        return ns

    def run(self):
        # regular mode: raise but not check signature
        # strict mode: called at render time
        if self.static_analysis == "regular":
            self.source._check_notebook(raise_=True, check_signature=False)

        fd, tmp = tempfile.mkstemp(".py")
        os.close(fd)

        code = "\n\n".join(
            [
                c["source"]
                for c in self.source.nb_obj_rendered.cells
                if c["cell_type"] == "code"
            ]
        )

        cwd = str(self.source.loc.parent.resolve())
        orig_env = os.environ.copy()

        if "PYTHONPATH" not in orig_env:
            orig_env["PYTHONPATH"] = cwd
        else:
            orig_env["PYTHONPATH"] += os.pathsep + cwd

        tmp = Path(tmp)
        tmp.write_text(code)

        if self.source.language == "python":
            interpreter = _python_bin()
        elif self.source.language == "r":
            interpreter = "Rscript"
        else:
            raise ValueError("ScriptRunner only works with Python and R scripts")

        try:
            _run_script_in_subprocess(interpreter, tmp, cwd, orig_env)
        except Exception as e:
            raise TaskBuildError("Error when executing task" f" {self.name!r}.") from e
        finally:
            tmp.unlink()


def _run_script_in_subprocess(interpreter, path, cwd, env):
    res = subprocess.run([interpreter, str(path)], cwd=cwd, env=env, stderr=PIPE)

    if res.returncode:
        stderr = res.stderr.decode()

        if "SyntaxError" in stderr:
            stderr += (
                "(Note: IPython magics are not supported in "
                "ScriptRunner, remove them or use the regular "
                "NotebookRunner)"
            )

        raise RuntimeError("Error while executing ScriptRunner:\n" f"{stderr}")


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
""".format(
        chdir_code(Path(".").resolve())
    )

    cell = nbformat_v.new_code_cell(source, metadata={"tags": ["debugging-settings"]})
    nb.cells.insert(0, cell)

    return nb


def _save():
    res = input(
        "Notebook changed, do you want to save changes "
        "in the original location? (injected parameters "
        "and debugging cells will be removed before "
        'saving). Enter "no" to skip saving changes, '
        'anything else will be interpreted as "yes": '
    )
    return res != "no"
