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
import ast
from inspect import getargspec
import tempfile
from pathlib import Path
from io import StringIO
import warnings

import parso
from papermill.parameterize import parameterize_notebook
import nbformat
import jupytext

from ploomber.exceptions import RenderError, SourceInitializationError
from ploomber.placeholders.Placeholder import Placeholder
from ploomber.util import requires
from ploomber.sources import Source
from ploomber.static_analysis.extractors import extractor_class_for_language


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

    Notes
    -----
    The render method prepares the notebook for execution: it adds the
    parameters and it makes sure kernelspec is defined
    """
    @requires([
        'parso', 'pyflakes', 'jupytext', 'nbformat', 'papermill',
        'jupyter_client'
    ])
    def __init__(self,
                 primitive,
                 hot_reload=False,
                 ext_in=None,
                 kernelspec_name=None,
                 static_analysis=False):
        # any non-py file must first be converted using jupytext, we need
        # that representation for validation, if input is already a .py file
        # do not convert. If passed a string, try to guess format using
        # jupytext. We also need ipynb representation for .develop(),
        # but do lazy loading in case we don't need both
        self._primitive = primitive

        # this happens if using SourceLoader
        if isinstance(primitive, Placeholder):
            self._path = primitive.path
            self._primitive = str(primitive)
        elif isinstance(primitive, str):
            self._path = None
            self._primitive = primitive
        elif isinstance(primitive, Path):
            self._path = primitive
            self._primitive = primitive.read_text()
        else:
            raise TypeError('Notebooks must be initialized from strings, '
                            'Placeholder or pathlib.Path, got {}'.format(
                                type(primitive)))

        self.static_analysis = static_analysis
        self._kernelspec_name = kernelspec_name
        self._hot_reload = hot_reload

        # TODO: validate ext_in values and extensions

        if self._path is None and hot_reload:
            raise ValueError('hot_reload only works in the notebook was '
                             'loaded from a file')

        if self._path is not None and ext_in is None:
            self._ext_in = self._path.suffix[1:]
        elif self._path is None and ext_in is None:
            raise ValueError('ext_in cannot be None if notebook is '
                             'initialized from a string')
        elif self._path is not None and ext_in is not None:
            raise ValueError('ext_in must be None if notebook is '
                             'initialized from a file')
        elif self._path is None and ext_in is not None:
            self._ext_in = ext_in

        # try to determine language based on extension, though this test
        # mught be inconclusive if dealing with a ipynb file
        self._language = determine_language(self._ext_in)

        self._python_repr = None
        self._loc = None
        self._rendered_nb_str = None
        self._params = None
        self._nb_repr = None
        self._nb_obj = None

        # this will raise an error if kernelspec_name is invalid
        self._get_nb_repr()

        self._post_init_validation(str(self._primitive))

    @property
    def primitive(self):
        if self._hot_reload:
            self._primitive = self._path.read_text()

        return self._primitive

    def render(self, params):
        """Render notebook (fill parameters using papermill)
        """
        self._params = json_serializable_params(params)
        self._render()

    def _render(self):
        import papermill as pm

        tmp_in = tempfile.mktemp('.ipynb')
        tmp_out = tempfile.mktemp('.ipynb')
        # _get_nb_repr uses hot_reload, this ensures we always get the latest
        # version
        Path(tmp_in).write_text(self._get_nb_repr())
        pm.execute_notebook(tmp_in,
                            tmp_out,
                            prepare_only=True,
                            parameters=self._params)

        tmp_out = Path(tmp_out)

        self._rendered_nb_str = tmp_out.read_text()

        Path(tmp_in).unlink()
        tmp_out.unlink()

        self._post_render_validation(self._params)

    # FIXME: looks like we are not using this, remove
    def _get_python_repr(self):
        """
        Returns the Python representation for this notebook, this is the
        raw source code passed, does not contain injected parameters
        """
        if self._python_repr is None:
            if self._ext_in == 'py':
                self._python_repr = self._primitive
            else:
                # convert from ipynb to notebook
                nb = nbformat.reads(self._primitive,
                                    as_version=nbformat.NO_CONVERT)
                self._python_repr = jupytext.writes(nb, fmt='py')

        return self._python_repr

    def _get_nb_repr(self):
        """
        Returns the notebook representation (JSON string), this is the raw
        source code passed, does not contain injected parameters.

        Adds kernelspec info if not present based on the kernelspec_name,
        this metadata is required for papermill to know which kernel to use.

        An exception is raised if we cannot determine kernel information.
        """
        # hot_reload causes to always re-evalaute the notebook representation
        if self._nb_repr is None or self._hot_reload:
            # this is the notebook node representation
            self._nb_obj = _to_nb_obj(
                self.primitive,
                ext=self._ext_in,
                # passing the underscored version
                # because that's the only one available
                # when this is initialized
                language=self._language,
                kernelspec_name=self._kernelspec_name)

            # always write from nb_obj, even if this was initialized
            # with a ipynb file, nb_obj contains kernelspec info
            self._nb_repr = nbformat.writes(self._nb_obj,
                                            version=nbformat.NO_CONVERT)

        return self._nb_repr

    def _post_init_validation(self, value):
        """
        Validate notebook after initialization (run pyflakes to detect
        syntax errors)
        """
        # NOTE: what happens if I pass source code with errors to parso?
        # maybe we don't need to use pyflakes after all
        # we can also use compile. can pyflakes detect things that
        # compile cannot?
        params_cell, _ = find_cell_with_tag(self._nb_obj, 'parameters')

        if params_cell is None:
            loc = ' "{}"'.format(self.loc) if self.loc else ''
            raise SourceInitializationError(
                'Notebook{} does not have a cell tagged '
                '"parameters"'.format(loc))

    def _post_render_validation(self, params):
        """
        Validate params passed against parameters in the notebook
        """
        if self.static_analysis:
            if self.language == 'python':
                # read the notebook with the injected parameters from the tmp
                # location
                nb_rendered = (nbformat.reads(self._rendered_nb_str,
                                              as_version=nbformat.NO_CONVERT))
                check_notebook(nb_rendered,
                               params,
                               filename=self._path or 'notebook')
            else:
                raise NotImplementedError(
                    'static_analysis is only implemented for Python notebooks'
                    ', set the option to False')

    @property
    def doc(self):
        # TODO: look for a cell tagged "docstring?"
        return None

    @property
    def loc(self):
        return self._path

    @property
    def name(self):
        return self._path.name

    @property
    def rendered_nb_str(self):
        if self._rendered_nb_str is None:
            raise RuntimeError('Attempted to get location for an unrendered '
                               'notebook, render it first')

        if self._hot_reload:
            self._render()

        return self._rendered_nb_str

    def __str__(self):
        # NOTE: the value returned here is used to determine code differences,
        # if this is initialized with a ipynb file, any change in the structure
        # will trigger execution, we could make this return a text-only version
        # depending on what we want we could either just concatenate the code
        # cells or use jupytext to keep metadata
        return self.primitive

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
            self._get_nb_repr()

            try:
                # make sure you return "r" instead of "R"
                return self._nb_obj.metadata.kernelspec.language.lower()
            except AttributeError:
                return None

        else:
            return self._language

    def _get_parameters_cell(self):
        self._get_nb_repr()
        cell, _ = find_cell_with_tag(self._nb_obj, tag='parameters')
        return cell.source

    # FIXME: A bit inefficient to initialize the extractor every time
    def extract_upstream(self):
        extractor_class = extractor_class_for_language(self.language)
        return extractor_class(self._get_parameters_cell()).extract_upstream()

    # FIXME: A bit inefficient to initialize the extractor every time
    def extract_product(self):
        extractor_class = extractor_class_for_language(self.language)
        return extractor_class(self._get_parameters_cell()).extract_product()


# FIXME: some of this only applies to Python notebooks (error about missing
# parameters cells applies to every notebook), make sure the source takes
# this into account, also check if there are any other functions that
# are python specific
def check_notebook(nb, params, filename):
    """
    Perform static analysis on a Jupyter notebook code cell sources

    Parameters
    ----------
    nb_source : str
        Jupyter notebook source code in jupytext's py format,
        must have a cell with the tag "parameters"

    params : dict
        Parameter that will be added to the notebook source

    filename : str
        Filename to identify pyflakes warnings and errors

    Raises
    ------
    RenderError
        If the notebook does not have a cell with the tag 'parameters',
        if the parameters in the notebook do not match the passed params or
        if pyflakes validation fails
    """
    # variable to collect all error messages
    error_message = '\n'

    params_cell, _ = find_cell_with_tag(nb, 'parameters')

    # compare passed parameters with declared
    # parameters. This will make our notebook behave more
    # like a "function", if any parameter is passed but not
    # declared, this will return an error message, if any parameter
    # is declared but not passed, a warning is shown
    res_params = compare_params(params_cell['source'], params)
    error_message += res_params

    # run pyflakes and collect errors
    res = check_source(nb, filename=filename)

    # pyflakes returns "warnings" and "errors", collect them separately
    if res['warnings']:
        error_message += 'pyflakes warnings:\n' + res['warnings']

    if res['errors']:
        error_message += 'pyflakes errors:\n' + res['errors']

    # if any errors were returned, raise an exception
    if error_message != '\n':
        raise RenderError(error_message)

    return True


def json_serializable_params(params):
    # papermill only allows JSON serializable parameters
    # convert Params object to dict
    params = params.to_dict()
    params['product'] = params['product'].to_json_serializable()

    if params.get('upstream'):
        params['upstream'] = {
            k: n.to_json_serializable()
            for k, n in params['upstream'].items()
        }
    return params


def compare_params(params_source, params):
    """
    Compare the parameters cell's source with the passed parameters, warn
    on missing parameter and raise error if an extra parameter was passed.
    """
    # params are keys in "params" dictionary
    params = set(params)

    # use parso to parse the "parameters" cell source code and get all
    # variable names declared
    declared = set(parso.parse(params_source).get_used_names().keys())

    # now act depending on missing variables and/or extra variables

    missing = declared - params
    extra = params - declared

    if missing:
        warnings.warn(
            'Missing parameters: {}, will use default value'.format(missing))

    if extra:
        return 'Passed non-declared parameters: {}'.format(extra)
    else:
        return ''


def check_source(nb, filename):
    """
    Run pyflakes on a notebook, wil catch errors such as missing passed
    parameters that do not have default values
    """
    from pyflakes.api import check as pyflakes_check
    from pyflakes.reporter import Reporter

    # concatenate all cell's source code in a single string
    source = '\n'.join([c['source'] for c in nb.cells])

    # this objects are needed to capture pyflakes output
    warn = StringIO()
    err = StringIO()
    reporter = Reporter(warn, err)

    # run pyflakes.api.check on the source code
    pyflakes_check(source, filename=filename, reporter=reporter)

    warn.seek(0)
    err.seek(0)

    # return any error messages returned by pyflakes
    return {
        'warnings': '\n'.join(warn.readlines()),
        'errors': '\n'.join(err.readlines())
    }


def _to_nb_obj(source, language, ext=None, kernelspec_name=None):
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
    import jupyter_client

    # let jupytext figure out the format
    nb = jupytext.reads(source, fmt=ext)

    kernel_name = determine_kernel_name(nb, language, kernelspec_name)

    # cannot keep going if we don't have the kernel name
    if kernel_name is None:
        raise SourceInitializationError(
            'Notebook does not contain kernelspec metadata and '
            'kernelspec_name was not specified, either add '
            'kernelspec info to your source file or specify '
            'a kernelspec by name. To see list of installed kernels run '
            '"jupyter kernelspec list" in the terminal (first column '
            'indicates the name). Python is usually named "python3", '
            'R usually "ir"')

    kernelspec = jupyter_client.kernelspec.get_kernel_spec(kernel_name)

    nb.metadata.kernelspec = {
        "display_name": kernelspec.display_name,
        "language": kernelspec.language,
        "name": kernel_name
    }

    return nb


def determine_kernel_name(nb, language, kernelspec_name):
    """
    Try to determine kernel name, first check notebook metadata, then
    check if the user explicitly provided a kernelspec_name. If None of this
    works use the language info, which should not be empty by the time this
    is executed
    """
    try:
        return nb.metadata.kernelspec.name
    except AttributeError:
        pass

    if kernelspec_name is not None:
        return kernelspec_name

    # two most common cases
    lang2kernel = {'python': 'python3', 'r': 'ir'}

    if language in lang2kernel:
        return lang2kernel[language]

    is_python_ = is_python(nb)

    if is_python_:
        return 'python3'
    else:
        return None


def inject_cell(model, params):
    """Inject params (by adding a new cell) to a model

    Notes
    -----
    A model is different than a notebook:
    https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html
    """
    nb = nbformat.from_dict(model['content'])

    # papermill adds a bunch of things before calling parameterize_notebook
    # if we don't add those things, parameterize_notebook breaks
    # https://github.com/nteract/papermill/blob/0532d499e13e93d8990211be33e9593f1bffbe6c/papermill/iorw.py#L400
    if not hasattr(nb.metadata, 'papermill'):
        nb.metadata['papermill'] = {
            'parameters': dict(),
            'environment_variables': dict(),
            'version': None,
        }

    for cell in nb.cells:
        if not hasattr(cell.metadata, 'tags'):
            cell.metadata['tags'] = []

    params = json_serializable_params(params)

    comment = ('This cell was injected automatically based on your stated '
               'upstream dependencies (cell above) and pipeline.yaml '
               'preferences. It is temporary and will be removed when you '
               'save this notebook')

    # a PR was merged to include this, but it hasn't been released yet,
    # so we check here
    # https://github.com/nteract/papermill/pull/521
    if 'comment' in getargspec(parameterize_notebook).args:
        kwargs = {'comment': comment}
    else:
        kwargs = {}

    model['content'] = parameterize_notebook(nb,
                                             params,
                                             report_mode=False,
                                             **kwargs)


# FIXME: this is used in the task itself in the .develop() feature, maybe
# move there?
def _cleanup_rendered_nb(nb):
    cell, i = find_cell_with_tag(nb, 'injected-parameters')

    if i is not None:
        print('Removing injected-parameters cell...')
        nb['cells'].pop(i)

    cell, i = find_cell_with_tag(nb, 'debugging-settings')

    if i is not None:
        print('Removing debugging-settings cell...')
        nb['cells'].pop(i)

    # papermill adds "tags" to all cells that don't have them, remove them
    # if they are empty to avoid cluttering the script
    for cell in nb['cells']:
        if 'tags' in cell.get('metadata', {}):
            if not len(cell['metadata']['tags']):
                del cell['metadata']['tags']

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
        is_python_ = language == 'python'

    # no language defined in metadata, check if it's valid python
    if is_python_ is None:
        code_str = '\n'.join([c.source for c in nb.cells])

        try:
            ast.parse(code_str)
        except SyntaxError:
            is_python_ = False
        else:
            # there is a lot of R code which is also valid Python code! So let's
            # run a quick test. It is very unlikely to have "<-" in Python (
            # {less than} {negative} but extremely common {assignment}
            if '<-' not in code_str:
                is_python_ = True

    # inconclusive test...
    if is_python_ is None:
        is_python_ = False

    return is_python_


def find_cell_with_tag(nb, tag):
    """
    Find a cell with a given tag, returns a cell, index tuple. Otherwise
    (None, None)
    """
    for i, c in enumerate(nb['cells']):
        cell_tags = c['metadata'].get('tags')
        if cell_tags:
            if tag in cell_tags:
                return c, i

    return None, None


def determine_language(extension):
    """
    A function to determine programming language given file extension,
    returns programming language name (all lowercase) if could be determined,
    None if the test is inconclusive
    """
    if extension.startswith('.'):
        extension = extension[1:]

    mapping = {'py': 'python', 'r': 'r', 'R': 'r', 'Rmd': 'r', 'rmd': 'r'}

    # ipynb can be many languages, it must return None
    return mapping.get(extension)
