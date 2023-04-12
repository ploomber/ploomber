import ast
import re
import warnings
from io import StringIO

import parso
from pyflakes import api as pyflakes_api
from pyflakes.reporter import Reporter
from pyflakes.messages import (
    UndefinedName,
    UndefinedLocal,
    DuplicateArgument,
    ReturnOutsideFunction,
    YieldOutsideFunction,
    ContinueOutsideLoop,
    BreakOutsideLoop,
)

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

_IS_IPYTHON_CELL_MAGIC = r"^\s*%{2}[a-zA-Z]+"
_IS_IPYTHON_LINE_MAGIC = r"^\s*%{1}[a-zA-Z]+"
_IS_INLINE_SHELL = r"^\s*!{1}.+"

HAS_INLINE_PYTHON = {"%%capture", "%%timeit", "%%time", "%time", "%timeit"}


def _process_messages(mesages):
    return "\n".join(str(msg) for msg in mesages)


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
        if isinstance(message, UndefinedName) and message.message_args == ("upstream",):
            message.message = (
                message.message + ". Did you forget to declare upstream dependencies?"
            )

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
        self._stdout.write("\n")

    def unexpectedError(self, *args, **kwargs):
        """pyflakes calls this when ast.parse raises an unexpected error"""
        self._unexpected = True
        return super().unexpectedError(*args, **kwargs)

    def syntaxError(self, *args, **kwargs):
        """pyflakes calls this when ast.parse raises a SyntaxError"""
        self._syntax = True
        return super().syntaxError(*args, **kwargs)

    def _seek_zero(self):
        self._stdout.seek(0)
        self._stderr.seek(0)

    def _make_error_message(self, error):
        return (
            "An error happened when checking the source code. "
            f"\n{error}\n\n"
            "(if you want to disable this check, set "
            'static_analysis to "disable" in the task declaration)'
        )

    def _check(self, raise_):
        self._seek_zero()

        # syntax errors are stored in _stderr
        # https://github.com/PyCQA/pyflakes/blob/master/pyflakes/api.py

        error_message = "\n".join(self._stderr.readlines())

        if self._syntax:
            msg = self._make_error_message(error_message)

            if raise_:
                raise SyntaxError(msg)
            else:
                warnings.warn(msg)

        elif self._unexpected:
            warnings.warn(
                "An unexpected error happened "
                f"when analyzing code: {error_message.strip()!r}"
            )
        else:
            errors, warnings_ = process_errors_and_warnings(self._stdout_raw)

            if warnings_:
                warnings.warn(warnings_)

            if errors:
                msg = self._make_error_message(errors)

                if raise_:
                    raise RenderError(msg)
                else:
                    warnings.warn(msg)


def check_notebook(nb, params, filename, raise_=True, check_signature=True):
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

    raise_ : bool, default=True
        If True, raises an Exception if it encounters errors, otherwise a
        warning

    Raises
    ------
    SyntaxError
        If the notebook's code contains syntax errors

    TypeError
        If params and nb do not match (unexpected or missing parameters)

    RenderError
        When certain pyflakes errors are detected (e.g., undefined name)
    """
    params_cell, _ = find_cell_with_tag(nb, "parameters")
    check_source(nb, raise_=raise_)

    if check_signature:
        check_params(params, params_cell["source"], filename, warn=not raise_)


def check_source(nb, raise_=True):
    """
    Run pyflakes on a notebook, wil catch errors such as missing passed
    parameters that do not have default values
    """
    # concatenate all cell's source code in a single string
    source_code = "\n".join(
        [
            _comment_if_ipython_magic(c["source"])
            for c in nb.cells
            if c.cell_type == "code"
        ]
    )

    # this objects are needed to capture pyflakes output
    reporter = MyReporter()

    # run pyflakes.api.check on the source code
    pyflakes_api.check(source_code, filename="", reporter=reporter)

    reporter._check(raise_)


def _comment(line):
    """Comments a line"""
    return f"# {line}"


def _comment_if_ipython_magic(source):
    """Comments lines into comments if they're IPython magics (cell level)"""
    # TODO: support for nested cell magics. e.g.,
    # %%timeit
    # %%timeit
    # something()
    lines_out = []
    comment_rest = False

    # TODO: inline magics should add a comment at the end of the line, because
    # the python code may change the dependency structure. e.g.,
    # %timeit z = x + y -> z = x + y # [magic] %timeit
    # note that this only applies to inline magics that take Python code as arg

    # NOTE: magics can take inputs but their outputs ARE NOT saved. e.g.,
    # %timeit x = y + 1
    # running such magic requires having y but after running it, x IS NOT
    # declared. But this is magic dependent %time x = y + 1 will add x to the
    # scope

    for line in source.splitlines():
        cell_magic = _is_ipython_cell_magic(line)

        if comment_rest:
            lines_out.append(_comment(line))
        else:
            line_magic = _is_ipython_line_magic(line)

            # if line magic, comment line
            if line_magic:
                lines_out.append(_comment(line))

            # if inline shell, comment line
            elif _is_inline_shell(line):
                lines_out.append(_comment(line))

            # if cell magic, comment line
            elif cell_magic in HAS_INLINE_PYTHON:
                lines_out.append(_comment(line))

            # if cell magic whose content *is not* Python, comment line and
            # all the remaining lines in the cell
            elif cell_magic:
                lines_out.append(_comment(line))
                comment_rest = True

            # otherwise, don't do anything
            else:
                lines_out.append(line)

    return "\n".join(lines_out)


def _is_ipython_line_magic(line):
    """
    Determines if the source line is an IPython magic. e.g.,

    %%bash
    for i in 1 2 3; do
      echo $i
    done
    """
    return re.match(_IS_IPYTHON_LINE_MAGIC, line) is not None


def _is_inline_shell(line):
    return re.match(_IS_INLINE_SHELL, line) is not None


def _is_ipython_cell_magic(source):
    """Determines if the source is an IPython cell magic. e.g.,

    %cd some-directory
    """
    m = re.match(_IS_IPYTHON_CELL_MAGIC, source.lstrip())

    if not m:
        return False

    return m.group()


class ParamsCell:
    """Parse variables defined in a  notebook cell"""

    def __init__(self, source):
        self._source = source
        # dot not complain if product or upstream are missing since
        # they are not user-defined params
        self._IGNORE = {"product", "upstream"}

        self._declared = _get_defined_variables(self._source)

        for key in self._IGNORE:
            self._declared.pop(key, None)

    def get_defined(self):
        return self._declared

    def get_missing(self, passed):
        passed = set(passed) - self._IGNORE
        return set(self._declared) - passed

    def get_unexpected(self, passed):
        passed = set(passed) - self._IGNORE
        return passed - set(self._declared)


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
    params_cell = ParamsCell(params_source)
    missing = params_cell.get_missing(passed)
    unexpected = params_cell.get_unexpected(passed)

    if missing or unexpected:
        errors = []

        if missing:
            errors.append(
                f"Missing params: {pretty_print.iterable(missing)} "
                "(to fix this, pass "
                f"{pretty_print.them_or_name(missing)} in "
                "the 'params' argument)"
            )
        if unexpected:
            first = list(unexpected)[0]
            errors.append(
                "Unexpected "
                f"params: {pretty_print.iterable(unexpected)} (to fix this, "
                f"add {pretty_print.them_or_name(unexpected)} to the "
                "'parameters' cell and assign the value as "
                f"None. e.g., {first} = None)"
            )

        msg = (
            f"Parameters "
            "declared in the 'parameters' cell do not match task "
            f"params. {pretty_print.trailing_dot(errors)} To disable this "
            "check, set 'static_analysis' to 'disable' in the "
            "task declaration."
        )

        if warn:
            warnings.warn(msg)
        else:
            raise TypeError(f"Error rendering notebook {str(filename)!r}. {msg}")


def _get_defined_variables(params_source):
    """
    Return the variables defined in a given source. If a name is defined more
    than once, it uses the last definition. Ignores anything other than
    variable assignments (e.g., function definitions, exceptions)
    """
    used_names = parso.parse(params_source).get_used_names()

    def _get_value(value):
        possible_literal = value.get_definition().children[-1].get_code().strip()

        try:
            # NOTE: this cannot parse dict(a=1, b=2)
            return ast.literal_eval(possible_literal)
        except ValueError:
            return None

    return {
        key: _get_value(value[-1])
        for key, value in used_names.items()
        if value[-1].is_definition() and value[-1].get_definition().type == "expr_stmt"
    }
