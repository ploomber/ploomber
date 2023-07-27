"""
On languages and kernels
------------------------
NotebookSource represents source code in a Jupyter notebook format (language
agnostic). Apart from .ipynb, we also support any other extension supported
by jupytext.

Given a notebook, we have to know which language it is written in to extract
upstream/product variables (though this only happens when the option of
extracting dependencies automatically is on), we also have to determine the
Jupyter kernel to use (this is always needed).

The unequivocal place to store this information is in the notebook metadata
section, but given that we advocate for the use of scripts (converted to
notebooks via jupytext), they most likely won't contain metadata (metadata
saving is turned off by default in jupytext), so we have to infer this
ourselves.

To make things more complex, jupytext adds its own metadata section but we are
ignoring that for now.

Given that there are many places where this information might be stored, we
have a few rules to automatically determine language and kernel given a
script/notebook.
"""
from functools import wraps
import ast
from pathlib import Path
import warnings
import logging
from contextlib import redirect_stdout
from io import StringIO
from copy import deepcopy
from papermill.parameterize import parameterize_notebook
import click
import nbformat
import jupytext
from jupytext import cli as jupytext_cli
from jupytext.formats import long_form_one_format, short_form_one_format
from jupytext.config import JupytextConfiguration
import parso

from ploomber.exceptions import SourceInitializationError
from ploomber.placeholders.placeholder import Placeholder
from ploomber_core.dependencies import requires
from ploomber.sources.abc import Source
from ploomber.sources.nb_utils import find_cell_with_tag, find_cell_with_tags
from ploomber.static_analysis.extractors import extractor_class_for_language
from ploomber.static_analysis.pyflakes import check_notebook
from ploomber.sources import docstring
from ploomber.io import pretty_print


class IgnoreBlackWarning(logging.Filter):
    def filter(self, record):
        return "Black is not installed" not in record.msg


logging.getLogger("papermill.translators").addFilter(IgnoreBlackWarning())


def _jupytext_fmt(primitive, extension):
    """
    Determine the jupytext fmt string to use based on the content and extension
    """
    if extension.startswith("."):
        extension = extension[1:]

    if extension != "ipynb":
        fmt, _ = jupytext.guess_format(primitive, f".{extension}")
        fmt_final = f"{extension}:{fmt}"
    else:
        fmt_final = ".ipynb"

    return fmt_final


# TODO: we should unit test that this function is called, as opposed to vanilla
# .read_text
def _read_primitive(path):
    """
    We read using the UTF-8 instead of the default encoding since notebooks are
    always stored in UTF-8.

    We can see this in nbformat, which always reads as UTF-8:
    https://github.com/jupyter/nbformat/blob/df63593b64a15ee1c37b522973c39e8674f93c5b/nbformat/__init__.py#L125

    Scripts are a different story since they may have other encodings, however,
    modern editors have UTF-8 as default (example: VSCode
    https://docs.microsoft.com/en-us/powershell/scripting/dev-cross-plat/vscode/understanding-file-encoding?view=powershell-7.2#configuring-vs-code)
    so it's safer to use UTF-8 than the default encoding.

    jupytext already does this:
    https://github.com/mwouts/jupytext/issues/896
    """
    return Path(path).read_text(encoding="utf-8")


def _get_last_cell(nb):
    """
    Get last cell, ignores cells with empty source (unless the notebook only
    has one cell and it's empty)
    """
    # iterate in reverse order
    for idx in range(-1, -len(nb.cells) - 1, -1):
        cell = nb.cells[idx]

        # only return it if it has some code
        if cell["source"].strip():
            return cell

    # otherwise return the first cell
    return nb.cells[0]


def _get_cell_suggestion(nb):
    format_name = (
        nb.metadata.get("jupytext", {})
        .get("text_representation", {})
        .get("format_name")
    )

    preamble = "Add a new cell with your code"

    if format_name == "light":
        message = (
            f"{preamble}:\n"
            + """
# + tags=["parameters"]
# your parameters here...
# -

# +
# your code here...
# -
"""
        )

    elif format_name == "percent":
        message = (
            f"{preamble}:\n"
            + """
# %% tags=["parameters"]
# your parameters here...

# %%
# your code here...
"""
        )
    else:
        message = preamble + "."

    return message


def requires_path(func):
    """
    Checks if NotebookSource instance was initialized from a file, raises
    an error if not
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._path is None:
            raise ValueError(
                f"Cannot use {func.__name__!r} if notebook was "
                "not initialized from a file"
            )

        return func(self, *args, **kwargs)

    return wrapper


class NotebookSource(Source):
    """
    A source object representing a jupyter notebook (or any format supported
    by jupytext)

    Parameters
    ----------
    hot_reload : bool, optional
        Makes the notebook always read the file before rendering

    kernelspec_name : str, optional
        Which kernel to use for executing the notebook, it overrides any
        existing kernelspec metadata in the notebook. If the notebook does
        not have kernelspec info, this parameter is required. Defaults to None.
        To see which kernelspecs are available run "jupyter kernelspec list"

    check_if_kernel_installed : bool, optional
        Check if the kernel is installed during initization

    Notes
    -----
    The render method prepares the notebook for execution: it adds the
    parameters and it makes sure kernelspec is defined
    """

    @requires(
        ["parso", "pyflakes", "jupytext", "nbformat", "papermill", "jupyter_client"]
    )
    def __init__(
        self,
        primitive,
        hot_reload=False,
        ext_in=None,
        kernelspec_name=None,
        static_analysis="regular",
        check_if_kernel_installed=True,
    ):
        # any non-py file must first be converted using jupytext, we need
        # that representation for validation, if input is already a .py file
        # do not convert. If passed a string, try to guess format using
        # jupytext. We also need ipynb representation for .develop(),
        # but do lazy loading in case we don't need both
        self._primitive = primitive
        self._check_if_kernel_installed = check_if_kernel_installed

        # this happens if using SourceLoader
        if isinstance(primitive, Placeholder):
            self._path = primitive.path
            self._primitive = str(primitive)
        elif isinstance(primitive, str):
            self._path = None
            self._primitive = primitive
        elif isinstance(primitive, Path):
            self._path = primitive

            if primitive.is_dir():
                raise SourceInitializationError(
                    f"Failed to initialize {str(primitive)!r}. "
                    "Expected a file, got a directory."
                    + _suggest_ploomber_scaffold_is_dir()
                )

            if not primitive.exists():
                raise SourceInitializationError(
                    f"Failed to initialize {str(primitive)!r}. "
                    "File does not exist." + _suggest_ploomber_scaffold_missing_file()
                )

            self._primitive = _read_primitive(primitive)
        else:
            raise TypeError(
                "Notebooks must be initialized from strings, "
                "Placeholder or pathlib.Path, got {}".format(type(primitive))
            )

        static_analysis_vals = {"disable", "regular", "strict"}

        if static_analysis not in static_analysis_vals:
            raise ValueError(
                f"{static_analysis!r} is not a "
                "valid 'static_analysis' value, choose one from: "
                f"{pretty_print.iterable(static_analysis_vals)}"
            )

        self.static_analysis = static_analysis
        self._kernelspec_name = kernelspec_name
        self._hot_reload = hot_reload

        # TODO: validate ext_in values and extensions

        if self._path is None and hot_reload:
            raise ValueError(
                "hot_reload only works in the notebook was " "loaded from a file"
            )

        if self._path is not None and ext_in is None:
            self._ext_in = self._path.suffix[1:]
        elif self._path is None and ext_in is None:
            if Path(self._primitive).exists():
                path = str(self._primitive)
                raise ValueError(
                    f"The file {path!r} you passed looks like "
                    "a path to a file. Perhaps you meant passing a "
                    "pathlib.Path object? Example:\n\n"
                    "from pathlib import Path\n"
                    f"NotebookRunner(Path({path!r}))"
                )

            else:
                raise ValueError(
                    '"ext_in" cannot be None if the notebook is '
                    "initialized from a string. Either pass "
                    "a pathlib.Path object with the notebook file "
                    "location or pass the source code as string "
                    'and include the "ext_in" parameter'
                )

        elif self._path is not None and ext_in is not None:
            raise ValueError(
                '"ext_in" must be None if notebook is '
                "initialized from a pathlib.Path object"
            )
        elif self._path is None and ext_in is not None:
            self._ext_in = ext_in

        # try to determine language based on extension, though this test
        # might be inconclusive if dealing with a ipynb file, though we only
        # use this to determine the appropriate jupyter kernel when
        # initializing from a string, when initializing from files, the
        # extension is used to determine the kernel
        self._language = determine_language(self._ext_in)

        self._loc = None
        self._params = None

        self._nb_str_unrendered = None
        self._nb_obj_unrendered = None
        self._nb_str_rendered = None
        self._nb_obj_rendered = None

        # this will raise an error if kernelspec_name is invalid
        self._read_nb_str_unrendered()

        self._post_init_validation(str(self._primitive))

    @property
    def primitive(self):
        if self._hot_reload:
            self._primitive = _read_primitive(self._path)

        return self._primitive

    def render(self, params):
        """Render notebook (fill parameters using papermill)"""
        self._params = json_serializable_params(params)
        self._render()

    def _render(self):
        # _read_nb_str_unrendered uses hot_reload, this ensures we always get
        # the latest version
        _, nb = self._read_nb_str_unrendered()

        if nb.cells and "parameters" in _get_last_cell(nb).metadata.get("tags", []):
            cell_suggestion = _get_cell_suggestion(nb)
            kind = "notebook" if self._ext_in == "ipynb" else "script"
            raise SourceInitializationError(
                f"Error processing {str(self._path)!r}: the last cell "
                f"in the {kind} is the parameters cell. {cell_suggestion}"
            )

        # this is needed for parameterize_notebook to work
        for cell in nb.cells:
            if not hasattr(cell.metadata, "tags"):
                cell.metadata["tags"] = []
        nb.metadata["papermill"] = dict()

        # NOTE: we use parameterize_notebook instead of execute_notebook
        # with the prepare_only option because the latter adds a "papermill"
        # section on each cell's metadata, which makes it too verbose when
        # using NotebookRunner.develop() when the source is script (each cell
        # will have an empty "papermill" metadata dictionary)

        # NOTE: in papermill 2.4.0, this method no longer creates a deepcopy
        nb = parameterize_notebook(deepcopy(nb), self._params)

        # delete empty tags to prevent cluttering the notebooks
        for cell in nb.cells:
            if not len(cell.metadata["tags"]):
                cell.metadata.pop("tags")

        self._nb_str_rendered = nbformat.writes(nb)
        self._post_render_validation()

    def _read_nb_str_unrendered(self, force=False):
        """
        Returns the notebook representation (JSON string), this is the raw
        source code passed, does not contain injected parameters.

        Adds kernelspec info if not present based on the kernelspec_name,
        this metadata is required for papermill to know which kernel to use.

        An exception is raised if we cannot determine kernel information.
        """
        # hot_reload causes to always re-evalaute the notebook representation
        if self._nb_str_unrendered is None or self._hot_reload or force:
            if force:
                primitive = _read_primitive(self._path)
            else:
                primitive = self.primitive

            # this is the notebook node representation
            nb = _to_nb_obj(
                primitive,
                ext=self._ext_in,
                # passing the underscored version
                # because that's the only one available
                # when this is initialized
                language=self._language,
                kernelspec_name=self._kernelspec_name,
                check_if_kernel_installed=self._check_if_kernel_installed,
                path=self._path,
            )

            # if the user injected cells manually (with ploomber nb --inject)
            # the source will contain the injected cell, remove it because
            # it should not be considered part of the source code
            self._nb_obj_unrendered = _cleanup_rendered_nb(nb, print_=False)

            # get the str representation. always write from nb_obj, even if
            # this was initialized with a ipynb file, nb_obj contains
            # kernelspec info
            self._nb_str_unrendered = nbformat.writes(
                self._nb_obj_unrendered, version=nbformat.NO_CONVERT
            )

        return self._nb_str_unrendered, self._nb_obj_unrendered

    def _post_init_validation(self, value):
        """
        Validate notebook after initialization (run pyflakes to detect
        syntax errors)
        """
        pass

    def _validate_parameters_cell(self, extract_upstream=False, extract_product=False):
        """Check parameters call and add it when it's missing

        Parameters
        ----------
        extract_upstream : bool, default: False
            Flags used to determine the content of the parameters cell,
            only used if the notebook is missing the parameters cell
        extract_product : bool, default: False
            Same as extract_upstream
        """
        params_cell, _ = find_cell_with_tag(self._nb_obj_unrendered, "parameters")

        if params_cell is None:
            loc = pretty_print.try_relative_path(self.loc)
            add_parameters_cell(self.loc, extract_upstream, extract_product)
            click.secho(
                f"Notebook {loc} is missing the parameters cell, "
                "adding it at the top of the file...",
                fg="yellow",
            )

    def _post_render_validation(self):
        """
        Validate params passed against parameters in the notebook
        """
        # NOTE: maybe static_analysis = off should not turn off everything
        # but only warn

        # strict mode: raise and check signature
        # regular mode: _check_notebook called in .run
        if self.static_analysis == "strict":
            self._check_notebook(raise_=True, check_signature=True)
        else:
            # otherwise, only warn on unused parameters
            _warn_on_unused_params(self._nb_obj_unrendered, self._params)

    def _check_notebook(self, raise_, check_signature):
        if self.static_analysis and self.language == "python":
            # warn if errors (e.g., undeclared variables, syntax errors)
            check_notebook(
                self._nb_str_to_obj(self._nb_str_rendered),
                self._params,
                filename=self._path or "notebook",
                raise_=raise_,
                check_signature=check_signature,
            )

    @property
    def doc(self):
        """
        Returns notebook docstring parsed either from a triple quoted string
        in the top cell or a top markdown markdown cell
        """
        return docstring.extract_from_nb(self._nb_obj_unrendered)

    @property
    def loc(self):
        return self._path

    @property
    def name(self):
        # filename without extension(e.g., plot.py -> plot)
        if self._path:
            return self._path.stem

    @property
    def nb_str_rendered(self):
        """
        Returns the notebook (as a string) with parameters injected, hot
        reloadig if necessary
        """
        if self._nb_str_rendered is None:
            raise RuntimeError(
                "Attempted to get location for an unrendered "
                "notebook, render it first"
            )

        if self._hot_reload:
            self._render()

        return self._nb_str_rendered

    @property
    def nb_obj_rendered(self):
        """
        Returns the notebook (as an objet) with parameters injected, hot
        reloadig if necessary
        """
        if self._nb_obj_rendered is None:
            # using self.nb_str_rendered triggers hot reload if needed
            self._nb_obj_rendered = self._nb_str_to_obj(self.nb_str_rendered)

        return self._nb_obj_rendered

    def __str__(self):
        # reload if empty or hot_reload=True
        self._read_nb_str_unrendered()

        # FIXME: this should ignore changes to the markdown cells
        return "\n".join([c.source for c in self._nb_obj_unrendered.cells])

    def __repr__(self):
        if self.loc is not None:
            return "{}('{}')".format(type(self).__name__, self.loc)
        else:
            return "{}(loaded from string)".format(type(self).__name__)

    @property
    def variables(self):
        raise NotImplementedError

    @property
    def extension(self):
        # this can be Python, R, Julia, etc. We are handling them the same,
        # for now, no normalization can be done.
        # One approach is to use the ext if loaded from file, otherwise None
        return None

    # FIXME: add this to the abstract class, probably get rid of "extension"
    # since it's not informative (ipynb files can be Python, R, etc)
    @property
    def language(self):
        """
        Notebook Language (Python, R, etc), this is a best-effort property,
        can be None if we could not determine the language
        """
        if self._language is None:
            self._read_nb_str_unrendered()

            try:
                # make sure you return "r" instead of "R"
                return self._nb_obj_unrendered.metadata.kernelspec.language.lower()
            except AttributeError:
                return None

        else:
            return self._language

    def _nb_str_to_obj(self, nb_str):
        return nbformat.reads(nb_str, as_version=nbformat.NO_CONVERT)

    def _get_parameters_cell(self, force=False):
        self._read_nb_str_unrendered(force=force)
        cell, _ = find_cell_with_tag(self._nb_obj_unrendered, tag="parameters")
        return cell.source if cell else None

    def extract_upstream(self, force=False):
        extractor_class = extractor_class_for_language(self.language)
        return extractor_class(
            self._get_parameters_cell(force=force)
        ).extract_upstream()

    def extract_product(self):
        extractor_class = extractor_class_for_language(self.language)
        return extractor_class(self._get_parameters_cell()).extract_product()

    @requires_path
    def save_injected_cell(self, **kwargs):
        """
        Inject cell, overwrite the source file (and any paired files)
        """
        fmt_ = _jupytext_fmt(self._primitive, self._ext_in)

        # add metadata to flag that the cell was injected manually
        recursive_update(
            self.nb_obj_rendered,
            dict(metadata=dict(ploomber=dict(injected_manually=True))),
        )

        # Are we updating a text file that has a metadata filter? If so,
        # add ploomber as a section that must be stored
        if (
            self.nb_obj_rendered.metadata.get("jupytext", {}).get(
                "notebook_metadata_filter"
            )
            == "-all"
        ):
            recursive_update(
                self.nb_obj_rendered,
                dict(
                    metadata=dict(
                        jupytext=dict(notebook_metadata_filter="ploomber,-all")
                    )
                ),
            )

        # overwrite
        jupytext.write(self.nb_obj_rendered, self._path, fmt=fmt_)

        # overwrite all paired files
        for path, fmt_ in iter_paired_notebooks(
            self.nb_obj_rendered, fmt_, self._path.stem
        ):
            # get absolute path for each notebook
            path = Path(self._path.parent / path)
            jupytext.write(self.nb_obj_rendered, fp=path, fmt=fmt_)

    @requires_path
    def remove_injected_cell(self):
        """
        Delete injected cell, overwrite the source file (and any paired files)
        """
        nb_clean = _cleanup_rendered_nb(self._nb_obj_unrendered)

        # remove metadata
        recursive_update(
            nb_clean, dict(metadata=dict(ploomber=dict(injected_manually=None)))
        )

        fmt_ = _jupytext_fmt(self._primitive, self._ext_in)

        # overwrite
        jupytext.write(nb_clean, self._path, fmt=fmt_)

        # overwrite all paired files
        for path, fmt_ in iter_paired_notebooks(
            self._nb_obj_unrendered, fmt_, self._path.stem
        ):
            # get absolute path for each notebook
            path = Path(self._path.parent / path)
            jupytext.write(nb_clean, fp=path, fmt=fmt_)

    @requires_path
    def format(self, fmt, entry_point):
        """Change source format

        Returns
        -------
        str
            The path if the extension changed, None otherwise
        """
        nb_clean = _cleanup_rendered_nb(self._nb_obj_unrendered)

        ext_file = self._path.suffix
        ext_format = long_form_one_format(fmt)["extension"]
        extension_changed = ext_file != ext_format

        if extension_changed:
            if Path(entry_point).is_file():
                path = self._path.with_suffix(ext_format)
                Path(self._path).unlink()
                modified_entry = Path(entry_point).read_text()
                main_file = f"{self.name}{ext_file}"
                if main_file in modified_entry:
                    modified_entry = modified_entry.replace(
                        main_file, f"{self.name}{ext_format}"
                    )
                    Path(entry_point).write_text(modified_entry)
                else:
                    click.secho(
                        f"{main_file} does not appear in entry-point"
                        f"please edit manually\n",
                        fg="yellow",
                    )
                    path = self._path
            else:
                click.secho(
                    "The entry-point is not a valid file, please"
                    " update the pipeline file extensions manually\n",
                    fg="yellow",
                )
                path = self._path
        else:
            path = self._path

        jupytext.write(nb_clean, path, fmt=fmt)

        return path if extension_changed else None

    @requires_path
    def pair(self, base_path):
        """Pairs with an ipynb file"""
        # TODO: add unit test
        if self._ext_in == "ipynb":
            raise ValueError(
                "pairing only works with .py files, got .ipynb. "
                "Yoy may convert the .ipynb to .py and try again."
            )

        fmt, _ = jupytext.guess_format(self._primitive, f".{self._ext_in}")
        fmt_ = f"{self._ext_in}:{fmt}"

        # mute jupytext's output
        with redirect_stdout(StringIO()):
            jupytext_cli.jupytext(
                args=["--set-formats", f"{base_path}//ipynb,{fmt_}", str(self._path)]
            )

    @requires_path
    def sync(self):
        """Pairs with and ipynb file"""
        # mute jupytext's output
        with redirect_stdout(StringIO()):
            jupytext_cli.jupytext(args=["--sync", str(self._path)])


def json_serializable_params(params):
    # papermill only allows JSON serializable parameters
    # convert Params object to dict
    params = params.to_dict()
    params["product"] = params["product"].to_json_serializable()

    if params.get("upstream"):
        params["upstream"] = params["upstream"].to_json_serializable()

    return params


def _to_nb_obj(
    source,
    language,
    ext=None,
    kernelspec_name=None,
    check_if_kernel_installed=True,
    path=None,
):
    """
    Convert to jupyter notebook via jupytext, if the notebook does not contain
    kernel information and the user did not pass a kernelspec_name explicitly,
    we will try to infer the language and select a kernel appropriately.

    If a valid kernel is found, it is added to the notebook. If none of this
    works, an exception is raised.

    If also converts the code string to its notebook node representation,
    adding kernel data accordingly.

    Parameters
    ----------
    source : str
        Jupyter notebook (or jupytext compatible formatted) document

    language : str
        Programming language

    path : str, default=None
        Script/notebook path. If not None, it's used to throw an informative
        error if the notebook fails to load

    Returns
    -------
    nb
        Notebook object


    Raises
    ------
    RenderError
        If the notebook has no kernelspec metadata and kernelspec_name is
        None. A notebook without kernelspec metadata will not display in
        jupyter notebook correctly. We have to make sure all notebooks
        have this.
    """
    import jupytext

    # let jupytext figure out the format
    try:
        nb = jupytext.reads(source, fmt=ext)
    except Exception as e:
        what = "notebook" if ext == "ipynb" else "script"
        err = f"Failed to read {what}"

        if path is not None:
            err += f" from {str(path)!r}"

        raise SourceInitializationError(err) from e

    # NOTE: I can add the cell with parameters here, but what happens if
    # extract_upstream is false? would that be a problem?

    check_nb_kernelspec_info(
        nb, kernelspec_name, ext, language, check_if_installed=check_if_kernel_installed
    )

    return nb


def check_nb_kernelspec_info(
    nb, kernelspec_name, ext, language, check_if_installed=True
):
    """Make sure the passed notebook has kernel info

    Parameters
    ----------
    check_if_installed : bool
        Also check if the kernelspec is installed, nb.metadata.kernelspec
        to be replaced by whatever information jupyter returns when requesting
        the kernelspec
    """
    import jupyter_client

    kernel_name = determine_kernel_name(nb, kernelspec_name, ext, language)

    # cannot keep going if we don't have the kernel name
    if kernel_name is None:
        raise SourceInitializationError(
            "Notebook does not contain kernelspec metadata and "
            "kernelspec_name was not specified, either add "
            "kernelspec info to your source file or specify "
            "a kernelspec by name. To see list of installed kernels run "
            '"jupyter kernelspec list" in the terminal (first column '
            'indicates the name). Python is usually named "python3", '
            'R usually "ir"'
        )

    if check_if_installed:
        kernelspec = jupyter_client.kernelspec.get_kernel_spec(kernel_name)

        nb.metadata.kernelspec = {
            "display_name": kernelspec.display_name,
            "language": kernelspec.language,
            "name": kernel_name,
        }
    else:
        if "metadata" not in nb:
            nb["metadata"] = dict()

        if "kernelspec" not in nb["metadata"]:
            nb["metadata"]["kernelspec"] = dict()

        # we cannot ask jupyter, so we fill this in ourselves
        nb.metadata.kernelspec = {
            "display_name": "R" if kernel_name == "ir" else "Python 3",
            "language": "R" if kernel_name == "ir" else "python",
            "name": kernel_name,
        }


def determine_kernel_name(nb, kernelspec_name, ext, language):
    """
    Determines the kernel name by using the following data (returns whatever
    gives kernel info first): 1) explicit kernel from the user 2) notebook's
    metadata 3) file extension 4) language 5) best guess
    """
    # explicit kernelspec name
    if kernelspec_name is not None:
        return kernelspec_name

    # use metadata info
    try:
        return nb.metadata.kernelspec.name
    except AttributeError:
        pass

    # use language from extension if passed, otherwise use language variable
    if ext:
        language = determine_language(ext)

    lang2kernel = {"python": "python3", "r": "ir"}

    if language in lang2kernel:
        return lang2kernel[language]

    # nothing worked, try to guess if it's python...
    is_python_ = is_python(nb)

    if is_python_:
        return "python3"
    else:
        return None


def inject_cell(model, params):
    """Inject params (by adding a new cell) to a model

    Notes
    -----
    A model is different than a notebook:
    https://jupyter-server.readthedocs.io/en/stable/developers/contents.html
    """
    nb = nbformat.from_dict(model["content"])

    # we must ensure nb has kernelspec info, otherwise papermill will fail to
    # parametrize
    ext = model["name"].split(".")[-1]
    check_nb_kernelspec_info(nb, kernelspec_name=None, ext=ext, language=None)

    # papermill adds a bunch of things before calling parameterize_notebook
    # if we don't add those things, parameterize_notebook breaks
    # https://github.com/nteract/papermill/blob/0532d499e13e93d8990211be33e9593f1bffbe6c/papermill/iorw.py#L400
    if not hasattr(nb.metadata, "papermill"):
        nb.metadata["papermill"] = {
            "parameters": dict(),
            "environment_variables": dict(),
            "version": None,
        }

    for cell in nb.cells:
        if not hasattr(cell.metadata, "tags"):
            cell.metadata["tags"] = []

    params = json_serializable_params(params)

    comment = (
        "This cell was injected automatically based on your stated "
        "upstream dependencies (cell above) and pipeline.yaml "
        "preferences. It is temporary and will be removed when you "
        "save this notebook"
    )

    model["content"] = parameterize_notebook(
        nb, params, report_mode=False, comment=comment
    )


def _cleanup_rendered_nb(nb, print_=True):
    """
    Cleans up a rendered notebook object. Removes cells with tags:
    injected-parameters, debugging-settings, and metadata injected by
    papermill
    """
    out = find_cell_with_tags(nb, ["injected-parameters", "debugging-settings"])

    if print_:
        for key in out.keys():
            print(f"Removing {key} cell...")

    idxs = set(cell["index"] for cell in out.values())

    nb["cells"] = [cell for idx, cell in enumerate(nb["cells"]) if idx not in idxs]

    # papermill adds "tags" to all cells that don't have them, remove them
    # if they are empty to avoid cluttering the script
    for cell in nb["cells"]:
        if "tags" in cell.get("metadata", {}):
            if not len(cell["metadata"]["tags"]):
                del cell["metadata"]["tags"]

    return nb


def is_python(nb):
    """
    Determine if the notebook is Python code for a given notebook object, look
    for metadata.kernelspec.language first, if not defined, try to guess if
    it's Python, it's conservative and it returns False if the code is valid
    Python but contains (<-), in which case it's much more likely to be R
    """
    is_python_ = None

    # check metadata first
    try:
        language = nb.metadata.kernelspec.language
    except AttributeError:
        pass
    else:
        is_python_ = language == "python"

    # no language defined in metadata, check if it's valid python
    if is_python_ is None:
        code_str = "\n".join([c.source for c in nb.cells])

        try:
            ast.parse(code_str)
        except SyntaxError:
            is_python_ = False
        else:
            # there is a lot of R code which is also valid Python code! So
            # let's
            # run a quick test. It is very unlikely to have "<-" in Python (
            # {less than} {negative} but extremely common {assignment}
            if "<-" not in code_str:
                is_python_ = True

    # inconclusive test...
    if is_python_ is None:
        is_python_ = False

    return is_python_


def determine_language(extension):
    """
    A function to determine programming language given file extension,
    returns programming language name (all lowercase) if could be determined,
    None if the test is inconclusive
    """
    if extension.startswith("."):
        extension = extension[1:]

    mapping = {"py": "python", "r": "r", "R": "r", "Rmd": "r", "rmd": "r"}

    # ipynb can be many languages, it must return None
    return mapping.get(extension)


def recursive_update(target, update):
    """Recursively update a dictionary. Taken from jupytext.header"""
    for key in update:
        value = update[key]
        if value is None:
            # remove if it exists
            target.pop(key, None)
        elif isinstance(value, dict):
            target[key] = recursive_update(target.get(key, {}), value)
        else:
            target[key] = value
    return target


def parse_jupytext_format(fmt, name):
    """
    Parse a jupytext format string (such as notebooks//ipynb) and return the
    path to the file and the extension
    """
    fmt_parsed = long_form_one_format(fmt)
    path = Path(fmt_parsed["prefix"], f'{name}{fmt_parsed["extension"]}')
    del fmt_parsed["prefix"]
    return path, short_form_one_format(fmt_parsed)


def iter_paired_notebooks(nb, fmt_, name):
    formats = nb.metadata.get("jupytext", {}).get("formats", "")

    if not formats:
        return

    formats = formats.split(",")

    formats.remove(fmt_)

    # overwrite all paired files
    for path, fmt_current in (parse_jupytext_format(fmt, name) for fmt in formats):
        yield path, fmt_current


def _nb2codestr(nb):
    return "\n".join([c.source for c in nb.cells if c.cell_type == "code"])


def _warn_on_unused_params(nb, params):
    nb = deepcopy(nb)
    _, idx = find_cell_with_tag(nb, "parameters")

    # the notebooks might not have a parameters cell
    if idx is None:
        return

    del nb.cells[idx]

    code = _nb2codestr(nb)

    # NOTE: if there a syntax error we cannot accurately check this
    m = parso.parse(code)
    names = set(m.get_used_names())

    # remove product since it may not be required
    # FIXME: maybe only remove it if it's a dictionary with >2 keys
    unused = set(params) - names - {"product"}

    if unused:
        warnings.warn(
            "These parameters are not used in the "
            f"task's source code: {pretty_print.iterable(unused)}"
        )


def add_parameters_cell(path, extract_upstream=False, extract_product=False):
    """
    Add parameters cell to a script/notebook in the given path, overwrites the
    original file
    """
    source = ""

    if extract_upstream:
        source += """\
# declare a list tasks whose products you want to use as inputs
upstream = None
"""
    if extract_product:
        source += """\
# declare a dictionary with the outputs of this task
product = None
"""

    if not extract_upstream and not extract_product:
        source += "# add default values for parameters here"

    c = JupytextConfiguration()
    c.notebook_metadata_filter
    c.cell_metadata_filter = "all"

    nb = jupytext.read(path)
    new_cell = nbformat.v4.new_code_cell(source, metadata={"tags": ["parameters"]})
    nb.cells.insert(0, new_cell)
    jupytext.write(nb, path, config=c)


def _suggest_ploomber_scaffold_missing_file():
    if Path("pipeline.yaml").is_file():
        return "\nTo create it, run: ploomber scaffold"
    else:
        return ""


def _suggest_ploomber_scaffold_is_dir():
    if Path("pipeline.yaml").is_file():
        return "\nTo create it, delete the directory, " "then run: ploomber scaffold"
    else:
        return ""
