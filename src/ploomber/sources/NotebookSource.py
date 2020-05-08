import tempfile
from pathlib import Path
from io import StringIO
import warnings

from ploomber.exceptions import SourceInitializationError
from ploomber.templates.Placeholder import Placeholder
from ploomber.util import requires
from ploomber.sources.sources import Source


class NotebookSource(Source):
    """
    A source object representing a jupyter notebook (or any format supported
    by jupytext)
    """

    @requires(['parso', 'pyflakes', 'jupytext', 'nbformat', 'papermill',
               'jupyter_client'])
    def __init__(self, value, ext_in=None, kernelspec_name=None):
        # any non-py file must first be converted using jupytext, we need
        # that representation for validation, if input is already a .py file
        # do not convert. If passed a string, try to guess format using
        # jupytext. We also need ipynb representation for .develop(),
        # but do lazy loading in case we don't need both
        self.placeholder = Placeholder(value)
        self._kernelspec_name = kernelspec_name

        if self.placeholder.needs_render:
            raise SourceInitializationError('The source for this task "{}"'
                                            ' must be a literal '
                                            .format(self.placeholder.value.raw))

        # this should not have tags, hence we can convert it right now
        self.value = str(self.placeholder)

        # TODO: validate ext_in values and extensions

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

    def _get_parameters(self):
        """
        Returns a dictionary with the declared parameters (variables in a cell
        tagged as "parameters")
        """
        pass

    def _get_python_repr(self):
        """
        Returns the Python representation for this notebook
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
        """Returns the notebook representation
        """
        import nbformat

        if self._nb_repr is None:
            if self._ext_in == 'ipynb':
                self._nb_repr = self.value
            else:
                nb = _load_nb(self.value, extension='py',
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

    def _post_render_validate(self):
        """
        Validate params passed against parameters in the notebook
        """
        # TODO: run check_notebook_source here
        # use papermill.execution_notebook in prepare_only mode
        # and replace the source, this way the user will be able
        # to see the source code with injected parameters,
        # then the cask just calls execute_notebook in the already-prepared
        # notebook
        pass

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


def check_notebook_source(nb_source, params, filename='notebook'):
    """
    Perform static analysis on a Jupyter notebook source raises
    an exception if validation fails

    Parameters
    ----------
    nb_source : str
        Jupyter notebook source code in jupytext's py format,
        must have a cell with the tag "parameters"

    params : dict
        Parameter that will be added to the notebook source

    filename : str
        Filename to identify pyflakes warnings and errors
    """
    import jupytext

    # parse the JSON string and convert it to a notebook object using jupytext
    nb = jupytext.reads(nb_source, fmt='py')

    # add a new cell just below the cell tagged with "parameters"
    # this emulates the notebook that papermill will run
    nb, params_cell = add_passed_parameters(nb, params)

    # run pyflakes and collect errors
    res = check_source(nb, filename=filename)
    error_message = '\n'

    # pyflakes returns "warnings" and "errors", collect them separately
    if res['warnings']:
        error_message += 'pyflakes warnings:\n' + res['warnings']

    if res['errors']:
        error_message += 'pyflakes errors:\n' + res['errors']

    # compare passed parameters with declared
    # parameters. This will make our notebook behave more
    # like a "function", if any parameter is passed but not
    # declared, this will return an error message, if any parameter
    # is declared but not passed, a warning is shown
    res_params = check_params(params_cell['source'], params)
    error_message += res_params

    # if any errors were returned, raise an exception
    if error_message != '\n':
        raise ValueError(error_message)

    return True


def check_params(params_source, params):
    """
    Compare the parameters cell's source with the passed parameters, warn
    on missing parameter and raise error if an extra parameter was passed.
    """
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


def add_passed_parameters(nb, params):
    """
    Insert a cell just below the one tagged with "parameters"

    Notes
    -----
    Insert a code cell with params, to simulate the notebook papermill
    will run. This is a simple implementation, for the actual one see:
    https://github.com/nteract/papermill/blob/master/papermill/parameterize.py
    """
    # find "parameters" cell
    idx, params_cell = _get_parameters_cell(nb)

    # convert the parameters passed to valid python code
    # e.g {'a': 1, 'b': 'hi'} to:
    # a = 1
    # b = 'hi'
    params_as_code = '\n'.join([_parse_token(k, v) for k, v in params.items()])

    # insert the cell with the passed parameters
    nb.cells.insert(idx + 1, {'cell_type': 'code', 'metadata': {},
                              'execution_count': None,
                              'source': params_as_code,
                              'outputs': []})
    return nb, params_cell


# TODO: integrate in the other function
def _get_parameters_cell(nb):
    """
    Iterate over cells, return the index and cell content
    for the first cell tagged "parameters", if not cell
    is found raise a ValueError
    """
    for i, c in enumerate(nb.cells):
        cell_tags = c.metadata.get('tags')
        if cell_tags:
            if 'parameters' in cell_tags:
                return i, c

    raise ValueError('Notebook does not have a cell tagged "parameters"')


# TODO: delete
def _parse_token(k, v):
    """
    Convert parameters to their Python code representation

    Notes
    -----
    This is a very simple way of doing it, for a more complete implementation,
    check out papermill's source code:
    https://github.com/nteract/papermill/blob/master/papermill/translators.py
    """
    return '{} = {}'.format(k, repr(v))


def _load_nb(source, extension, kernelspec_name=None):
    """Convert to jupyter notebook via jupytext

    Parameters
    ----------
    source : str
        Jupyter notebook (or jupytext compatible formatted) document

    extension : str
        Document format
    """
    import jupytext
    import jupyter_client
    # NOTE: how is this different to just doing fmt='.py'
    nb = jupytext.reads(source, fmt={'extension': '.'+extension})

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

    return nb
