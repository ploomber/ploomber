import tempfile
from pathlib import Path
from io import StringIO
import warnings

from ploomber.exceptions import RenderError, SourceInitializationError
from ploomber.placeholders.Placeholder import Placeholder
from ploomber.util import requires
from ploomber.sources import Source


# TODO: make sure this works with R, Julia and other notebooks supported
# by Jupyter
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

    @requires(['parso', 'pyflakes', 'jupytext', 'nbformat', 'papermill',
               'jupyter_client'])
    def __init__(self, primitive, hot_reload=False, ext_in=None,
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
                            'Placeholder or pathlib.Path, got {}'
                            .format(type(primitive)))

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

        self._post_init_validation(str(self._primitive))

        self._python_repr = None
        self._loc = None
        self._rendered_nb_str = None
        self._params = None
        self._nb_repr = None

        # this will raise an error if kernelspec_name is invalid
        self._get_nb_repr()

    @property
    def primitive(self):
        if self._hot_reload:
            self._primitive = self._path.read_text()

        return self._primitive

    def render(self, params):
        """Render notebook (fill parameters using papermill)
        """
        # papermill only allows JSON serializable parameters
        # convert Params object to dict
        params = params.to_dict()
        params['product'] = params['product'].to_json_serializable()

        if params.get('upstream'):
            params['upstream'] = {k: n.to_json_serializable() for k, n
                                  in params['upstream'].items()}

        self._params = params
        self._render()

    def _render(self):
        import papermill as pm

        tmp_in = tempfile.mktemp('.ipynb')
        tmp_out = tempfile.mktemp('.ipynb')
        # _get_nb_repr uses hot_reload, this ensures we always get the latest
        # version
        Path(tmp_in).write_text(self._get_nb_repr())
        pm.execute_notebook(tmp_in, tmp_out, prepare_only=True,
                            parameters=self._params)

        tmp_out = Path(tmp_out)

        self._rendered_nb_str = tmp_out.read_text()

        Path(tmp_in).unlink()
        tmp_out.unlink()

        self._post_render_validation(self._params)

    def _get_python_repr(self):
        """
        Returns the Python representation for this notebook, this is the
        raw source code passed, does not contain injected parameters
        """
        import jupytext
        import nbformat

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
        this metadata is required for papermill to know which kernel to use
        """
        import nbformat

        # hot_reload causes to always re-evalaute the notebook representation
        if self._nb_repr is None or self._hot_reload:
            if self._ext_in == 'ipynb':
                self._nb_repr = self.primitive
            else:
                nb = _to_nb_obj(self.primitive,
                                extension='py',
                                kernelspec_name=self._kernelspec_name)
                self._nb_repr = nbformat.writes(nb,
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
        pass

    def _post_render_validation(self, params):
        """
        Validate params passed against parameters in the notebook
        """
        if self.static_analysis:
            import nbformat
            # read the notebook with the injected parameters from the tmp
            # location
            nb_rendered = (nbformat
                           .reads(self._rendered_nb_str,
                                  as_version=nbformat.NO_CONVERT))
            check_notebook(nb_rendered, params,
                           filename=self._path or 'notebook')

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

    params_cell, _ = _find_cell_with_tag(nb, 'parameters')

    if params_cell is None:
        error_message += ('Notebook does not have a cell tagged '
                          '"parameters"')
    else:
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


def compare_params(params_source, params):
    """
    Compare the parameters cell's source with the passed parameters, warn
    on missing parameter and raise error if an extra parameter was passed.
    """
    # FIXME: we don't really need parso, we can just use the ast module
    import parso

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
    return {'warnings': '\n'.join(warn.readlines()),
            'errors': '\n'.join(err.readlines())}


def _to_nb_obj(source, extension, kernelspec_name=None):
    """
    Convert to jupyter notebook via jupytext, adding kernelspec details if
    missing

    Parameters
    ----------
    source : str
        Jupyter notebook (or jupytext compatible formatted) document

    extension : str
        Document format

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
    # NOTE: how is this different to just doing fmt='.py'
    nb = jupytext.reads(source, fmt={'extension': '.'+extension})
    nb_kernelspec = nb.metadata.get('kernelspec')

    if nb_kernelspec is None and kernelspec_name is None:
        raise SourceInitializationError(
            'Notebook does not contains kernelspec metadata and '
            'kernelspec_name was not specified, either add '
            'kernelspec info to your source file or specify '
            'a kernelspec by name')

    # only use kernelspec_name when there is no kernelspec info in the nb
    if nb_kernelspec is None and kernelspec_name is not None:
        k = jupyter_client.kernelspec.get_kernel_spec(kernelspec_name)

        nb.metadata.kernelspec = {
            "display_name": k.display_name,
            "language": k.language,
            "name": kernelspec_name
        }

    return nb


def _cleanup_rendered_nb(nb):
    cell, i = _find_cell_with_tag(nb, 'injected-parameters')

    if i is not None:
        print('Removing injected-parameters cell...')
        nb.cells.pop(i)

    cell, i = _find_cell_with_tag(nb, 'debugging-settings')

    if i is not None:
        print('Removing debugging-settings cell...')
        nb.cells.pop(i)

    return nb


def _find_cell_with_tag(nb, tag):
    """
    Find a cell with a given tag, returns a cell, index tuple. Otherwise
    (None, None)
    """
    for i, c in enumerate(nb.cells):
        cell_tags = c.metadata.get('tags')
        if cell_tags:
            if tag in cell_tags:
                return c, i

    return None, None
