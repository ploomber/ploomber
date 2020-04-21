from pathlib import Path
from io import StringIO
import warnings

import jupytext
import parso
from pyflakes.api import check as pyflakes_check
from pyflakes.reporter import Reporter

from ploomber.templates.Placeholder import Placeholder


class NotebookSource:
    """
    A source object representing a jupyter notebook (or any format supported
    by jupytext)
    """

    def __init__(self, value):
        # any non-py file must first be converted using jupytext, we need
        # that representation for validation, if input is already a .py file
        # do not convert. If passed a string, try to guess format using
        # jupytext
        self.value = Placeholder(value)
        self._post_init_validation(self.value)

    def _get_parameters(self):
        """
        Returns a dictionary with the declared parameters (variables in a cell
        tagged as "parameters")
        """
        pass

    def _to_python(self):
        """
        Returns the Python representation for this notebook
        """
        pass

    def _post_init_validate():
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


def to_python(value):
    """
    """
    # TODO: this should also handle the case when value is a Placeholder,
    # this happens when using sourceloader
    if isinstance(value, str):
        nb = jupytext.reads(value)
        return jupytext.writes(nb, fmt='py')

    # handle path
    else:
        # no need to convert
        if Path(value).suffix == '.py':
            return Path(value).read_text()

        nb = jupytext.read(value)
        return jupytext.writes(nb, fmt='py')


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
