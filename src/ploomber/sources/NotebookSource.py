import tempfile
from pathlib import Path
from io import StringIO
import warnings

from ploomber.exceptions import SourceInitializationError, RenderError
from ploomber.templates.Placeholder import Placeholder
from ploomber.util import requires
from ploomber.sources.sources import Source


# FIXME: for all tasks, there should be an easy way to check exactly
# which code will be run, make sure there is a consistent implementation accross
# products (and sources indirectly), including this one.
# Using a Placeholder for somthing that does not need tags is not right,
# make a very simple StaticPlaceholder class with the same API but simpler
# ignore all jinja stuff
class NotebookSource(Source):
    """
    A source object representing a jupyter notebook (or any format supported
    by jupytext)

    Parameters
    ----------
    hot_reload : bool, optional
        Makes the notebook always read the file before rendering

    Notes
    -----
    The render method prepares the notebook for execution: it adds the
    parameters and it makes sure kernelspec is defined
    """

    @requires(['parso', 'pyflakes', 'jupytext', 'nbformat', 'papermill',
               'jupyter_client'])
    def __init__(self, value, hot_reload=False, ext_in=None,
                 kernelspec_name=None,
                 static_analysis=False):
        # any non-py file must first be converted using jupytext, we need
        # that representation for validation, if input is already a .py file
        # do not convert. If passed a string, try to guess format using
        # jupytext. We also need ipynb representation for .develop(),
        # but do lazy loading in case we don't need both
        self.placeholder = Placeholder(value, hot_reload=hot_reload)
        self.static_analysis = static_analysis
        self._kernelspec_name = kernelspec_name
        self._hot_reload = hot_reload

        # FIXME: we should be doing this, might be the case that the notebook
        # is using jinja inside and {{}} will appear, we should just use
        # the raw value in the placeholder instead
        if self.placeholder.needs_render:
            raise SourceInitializationError('The source for this task "{}"'
                                            ' must be a literal '
                                            .format(self.placeholder.value.raw))

        # TODO: validate ext_in values and extensions

        if self.placeholder.path is None and hot_reload:
            raise ValueError('hot_reload only works in the notebook was '
                             'loaded from a file')

        if self.placeholder.path is not None and ext_in is None:
            self._ext_in = self.placeholder.path.suffix[1:]
        elif self.placeholder.path is None and ext_in is None:
            raise ValueError('ext_in cannot be None if notebook is '
                             'initialized from a string')
        elif self.placeholder.path is not None and ext_in is not None:
            raise ValueError('ext_in must be None if notebook is '
                             'initialized from a file')
        elif self.placeholder.path is None and ext_in is not None:
            self._ext_in = ext_in

        self._post_init_validation(self.value)

        self._python_repr = None
        self._nb_repr = None
        self._loc = None
        self._loc_rendered = None

    @property
    def value(self):
        return self.placeholder.raw

    def render(self, params):
        """Render notebook (fill parameters using papermill)
        """
        import papermill as pm

        # papermill only allows JSON serializable parameters
        # convert Params object to dict
        params = params.to_dict()
        params['product'] = params['product'].to_json_serializable()

        if params.get('upstream'):
            params['upstream'] = {k: n.to_json_serializable() for k, n
                                  in params['upstream'].items()}

        tmp_in = tempfile.mktemp('.ipynb')
        tmp_out = tempfile.mktemp('.ipynb')
        Path(tmp_in).write_text(self._get_nb_repr())
        pm.execute_notebook(tmp_in, tmp_out, prepare_only=True,
                            parameters=params)
        self._loc_rendered = tmp_out
        Path(tmp_in).unlink()

        self._post_render_validate(params)

    def _get_parameters(self):
        """
        Returns a dictionary with the declared parameters (variables in a cell
        tagged as "parameters")
        """
        pass

    def _get_python_repr(self):
        """
        Returns the Python representation for this notebook, this is the
        raw source code passed, does not contain injected parameters
        """
        import jupytext
        import nbformat

        if self._python_repr is None:
            if self._ext_in == 'py':
                self._python_repr = self.value
            else:
                # convert from ipynb to notebook
                nb = nbformat.reads(self.value, nbformat.current_nbformat)
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
                self._nb_repr = self.value
            else:
                nb = _to_nb_obj(self.value, extension='py',
                                kernelspec_name=self._kernelspec_name)
                writer = (nbformat
                          .versions[nbformat.current_nbformat]
                          .nbjson.JSONWriter())
                self._nb_repr = writer.writes(nb)

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

    def _post_render_validate(self, params):
        """
        Validate params passed against parameters in the notebook
        """
        if self.static_analysis:
            import nbformat
            # read the notebook with the injected parameters from the tmp
            # location
            nb_rendered = (nbformat
                           .reads(Path(self._loc_rendered).read_text(),
                                  nbformat.current_nbformat))
            check_notebook(nb_rendered, params,
                           filename=self.placeholder.path or 'notebook')

    @property
    def doc(self):
        return None

    @property
    def needs_render(self):
        return True

    @property
    def loc(self):
        return self.placeholder.path

    @property
    def loc_rendered(self):
        if self._loc_rendered is None:
            raise RuntimeError('Attempted to get location for an unrendered '
                               'notebook, render it first')
        return self._loc_rendered

    def __del__(self):
        if self._loc_rendered is not None:
            Path(self._loc_rendered).unlink()

    def __str__(self):
        return self.placeholder.raw


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

    if nb.metadata.get('kernelspec') is None and kernelspec_name is None:
        raise RenderError('Notebook does not contains kernelspec metadata and '
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
    for i, c in enumerate(nb.cells):
        cell_tags = c.metadata.get('tags')
        if cell_tags:
            if tag in cell_tags:
                return c, i

    return None, None
