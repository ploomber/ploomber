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

    def _check(self):
        self._seek_zero()

        # syntax errors are stored in _stderr
        # https://github.com/PyCQA/pyflakes/blob/master/pyflakes/api.py

        error_message = '\n'.join(self._stderr.readlines())

        if self._syntax:
            raise SyntaxError(error_message)
        elif self._unexpected:
            warnings.warn('An unexpected error happened '
                          f'when analyzing code: {error_message.strip()!r}')
        else:
            errors, warnings_ = process_errors_and_warnings(self._stdout_raw)

            if warnings_:
                warnings.warn(warnings_)

            if errors:
                raise RenderError(errors)


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
    source_code = '\n'.join(
        [c['source'] for c in nb.cells if c.cell_type == 'code'])

    # this objects are needed to capture pyflakes output
    reporter = MyReporter()

    # run pyflakes.api.check on the source code
    pyflakes_api.check(source_code, filename='', reporter=reporter)

    reporter._check()


def check_params(passed, params_source, filename):
    # dot not complain if product or upstream are missing since
    # they are not user-defined params
    IGNORE = {'product', 'upstream'}
    declared = set(parso.parse(params_source).get_used_names().keys()) - IGNORE
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
        raise TypeError(
            f"Error rendering notebook {str(filename)!r}, parameters "
            "declared in the 'parameters' cell do not match task "
            f"params. {pretty_print.trailing_dot(errors)}")
