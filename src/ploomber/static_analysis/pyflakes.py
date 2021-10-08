import re
import warnings
from io import StringIO

import parso
from pyflakes import api as pyflakes_api
from pyflakes.reporter import Reporter
from pyflakes.messages import (UndefinedName, UndefinedLocal,
                               DuplicateArgument, ReturnOutsideFunction,
                               YieldOutsideFunction, ContinueOutsideLoop,
                               BreakOutsideLoop)

from ploomber.exceptions import RenderError
from ploomber.io import pretty_print
from ploomber.sources.nb_utils import find_cell_with_tag

# messages: https://github.com/PyCQA/pyflakes/blob/master/pyflakes/messages.py
_ERRORS = (
    UndefinedName,
    UndefinedLocal,
    DuplicateArgument,
    ReturnOutsideFunction,
    YieldOutsideFunction,
    ContinueOutsideLoop,
    BreakOutsideLoop,
)

_CELL_IS_IPYTHON_CELL_MAGIC_REGEX = r'^%{2}[a-zA-Z]+'
_LINE_IS_IPYTHON_LINE_MAGIC_REGEX = r'^%{1}[a-zA-Z]+'


def _process_messages(mesages):
    return '\n'.join(str(msg) for msg in mesages)


def process_errors_and_warnings(messages):
    errors, warnings = [], []

    for message in messages:
        # By default, script templates include an
        # 'upstream = None' line in the parameters cell but users may delete
        # it. If that happens and they reference 'upstream' in the body of the
        # notebook but the task does not have upstream dependencies, the
        # injected cell won't add the upstream variable, causing an undefined
        # name error. To provide more context, we modify the original error and
        # add a hint. Note that this only happens when using the Python API
        # directly, since the Spec API requires an upstream = None placeholder
        if (isinstance(message, UndefinedName)
                and message.message_args == ('upstream', )):
            message.message = (
                message.message +
                '. Did you forget to declare upstream dependencies?')

        if isinstance(message, _ERRORS):
            errors.append(message)
        else:
            warnings.append(message)

    return _process_messages(errors), _process_messages(warnings)


# https://github.com/PyCQA/pyflakes/blob/master/pyflakes/reporter.py
class MyReporter(Reporter):
    def __init__(self):
        self._stdout = StringIO()
        self._stderr = StringIO()
        self._stdout_raw = []
        self._unexpected = False
        self._syntax = False

    def flake(self, message):
        self._stdout_raw.append(message)
        self._stdout.write(str(message))
        self._stdout.write('\n')

    def unexpectedError(self, *args, **kwargs):
        """pyflakes calls this when ast.parse raises an unexpected error
        """
        self._unexpected = True
        return super().unexpectedError(*args, **kwargs)

    def syntaxError(self, *args, **kwargs):
        """pyflakes calls this when ast.parse raises a SyntaxError
        """
        self._syntax = True
        return super().syntaxError(*args, **kwargs)

    def _seek_zero(self):
        self._stdout.seek(0)
        self._stderr.seek(0)

    def _make_error_message(self, error):
        return ('An error happened when checking the source code. '
                f'\n{error}\n\n'
                '(if you want to proceed with execution anyway, set '
                'static_analysis to False in the task declaration '
                'and execute again)')

    def _check(self):
        self._seek_zero()

        # syntax errors are stored in _stderr
        # https://github.com/PyCQA/pyflakes/blob/master/pyflakes/api.py

        error_message = '\n'.join(self._stderr.readlines())

        if self._syntax:
            raise SyntaxError(self._make_error_message(error_message))
        elif self._unexpected:
            warnings.warn('An unexpected error happened '
                          f'when analyzing code: {error_message.strip()!r}')
        else:
            errors, warnings_ = process_errors_and_warnings(self._stdout_raw)

            if warnings_:
                warnings.warn(warnings_)

            if errors:
                raise RenderError(self._make_error_message(errors))


def check_notebook(nb, params, filename):
    """
    Perform static analysis on a Jupyter notebook code cell sources

    Parameters
    ----------
    nb : NotebookNode
        Notebook object. Must have a cell with the tag "parameters"

    params : dict
        Parameter that will be added to the notebook source

    filename : str
        Filename to identify pyflakes warnings and errors

    Raises
    ------
    SyntaxError
        If the notebook's code contains syntax errors

    TypeError
        If params and nb do not match (unexpected or missing parameters)

    RenderError
        When certain pyflakes errors are detected (e.g., undefined name)
    """
    params_cell, _ = find_cell_with_tag(nb, 'parameters')
    check_source(nb)
    check_params(params, params_cell['source'], filename)


def check_source(nb):
    """
    Run pyflakes on a notebook, wil catch errors such as missing passed
    parameters that do not have default values
    """
    # concatenate all cell's source code in a single string
    source_code = '\n'.join([
        _comment_if_ipython_magic(c['source']) for c in nb.cells
        if c.cell_type == 'code'
    ])

    # this objects are needed to capture pyflakes output
    reporter = MyReporter()

    # run pyflakes.api.check on the source code
    pyflakes_api.check(source_code, filename='', reporter=reporter)

    reporter._check()


def _comment_if_ipython_magic(source):
    """Comments lines into comments if they're IPython magics
    """
    if _is_ipython_cell_magic(source):
        # comment all lines
        return '\n'.join(f'# {line}' for line in source.splitlines())
    else:
        return '\n'.join(
            (line if not _is_ipython_line_magic(line) else f'# {line}')
            for line in source.splitlines())


def _is_ipython_line_magic(line):
    """Determines if the source line is an IPython magic
    """
    return re.match(_LINE_IS_IPYTHON_LINE_MAGIC_REGEX, line) is not None


def _is_ipython_cell_magic(source):
    """Determines if the source is an IPython cell magic
    """
    return re.match(_CELL_IS_IPYTHON_CELL_MAGIC_REGEX,
                    source.lstrip()) is not None


def check_params(passed, params_source, filename, warn=False):
    """
    Check that parameters passed to the notebook match the ones defined
    in the parameters variable

    Parameters
    ----------
    passed : iterable
        Paramters passed to the notebook (params argument)

    params_source : str
        Parameters cell source code

    filename : str
        The task's filename. Only used for displaying in the error message

    warn : bool
        If False, it raises a TypeError if params do not match. If True,
        it displays a warning instead.

    Raises
    ------
    TypeError
        If passed parameters do not match variables declared in params_source
        and warn is False
    """
    # dot not complain if product or upstream are missing since
    # they are not user-defined params
    IGNORE = {'product', 'upstream'}

    declared = _get_defined_variables(params_source) - IGNORE
    passed = set(passed) - IGNORE
    missing = declared - passed
    unexpected = passed - declared

    if missing or unexpected:
        errors = []

        if missing:
            errors.append(f'Missing params: {pretty_print.iterable(missing)} '
                          '(to fix this, pass '
                          f'{pretty_print.them_or_name(missing)} in '
                          'the \'params\' argument)')
        if unexpected:
            first = list(unexpected)[0]
            errors.append(
                'Unexpected '
                f'params: {pretty_print.iterable(unexpected)} (to fix this, '
                f'add {pretty_print.them_or_name(unexpected)} to the '
                '\'parameters\' cell and assign the value as '
                f'None. e.g., {first} = None)')

        msg = (
            f"Parameters "
            "declared in the 'parameters' cell do not match task "
            f"params. {pretty_print.trailing_dot(errors)} To disable this "
            "check, set 'static_analysis' to False in the task declaration.")

        if warn:
            warnings.warn(msg)
        else:
            raise TypeError(
                f"Error rendering notebook {str(filename)!r}. {msg}")


def _get_defined_variables(params_source):
    """
    Return the variables defined in a given source. If a name is defined more
    than once, it uses the last definition. Ignores anything other than
    variable assignments (e.g., function definitions, exceptions)
    """
    used_names = parso.parse(params_source).get_used_names()
    return set(key for key, value in used_names.items()
               if value[-1].is_definition()
               and value[-1].get_definition().type == 'expr_stmt')
