import typing as t
from click.exceptions import ClickException
from click._compat import get_text_stderr
from click.utils import echo
from gettext import gettext as _


def _format_message(exception):
    if hasattr(exception, 'format_message'):
        return exception.format_message()
    else:
        return str(exception)


def _build_message(exception):
    msg = _format_message(exception)

    while exception.__cause__:
        msg += f'\n{_format_message(exception.__cause__)}'
        exception = exception.__cause__

    return msg


class BaseException(ClickException):
    """
    A subclass of ClickException that adds support for printing error messages
    from chained exceptions
    """
    def show(self, file: t.Optional[t.IO] = None) -> None:
        if file is None:
            file = get_text_stderr()

        message = _build_message(self)
        echo(_("Error: {message}").format(message=message), file=file)


class DAGRenderError(Exception):
    """Raise when a dag fails to build
    """
    pass


class DAGBuildError(Exception):
    """Raise when a dag fails to build
    """
    pass


class DAGBuildEarlyStop(Exception):
    """
    This is raised on purpose to signal that the DAG should not continue
    executing but is not considered a build error
    """
    pass


class TaskBuildError(Exception):
    """Raise when a task fails to build
    """
    pass


class TaskRenderError(Exception):
    """Raise when a task fails to render
    """
    pass


class RenderError(Exception):
    """Raise when a template fails to render
    """
    pass


class SourceInitializationError(Exception):
    """Raise when a source fails to initialize due to wrong parameters
    """
    pass


class CallbackSignatureError(Exception):
    """When a callback function does not have the right signature
    """
    pass


class CallbackCheckAborted(Exception):
    """
    Used by callback_check to signal that signature check is unfeasible because
    the user passed a DottedPath whose underlying function hasn't been imported
    """
    pass


class UpstreamKeyError(Exception):
    """
    Raised when trying to get an upstream dependency that does not exist,
    we have to implement a custom exception otherwise jinja is going
    to ignore our error messages (if we raise the usual KeyError).
    See: https://jinja.palletsprojects.com/en/2.11.x/templates/#variables
    """
    pass


class DAGSpecInitializationError(BaseException):
    """
    Raised when failing to initialize a DAGSpec object
    """
    pass


class DAGSpecInvalidError(Exception):
    """
    Raised when trying to find dagspec automatically but the file doesn't exist
    or there is an invalid configuration
    """
    pass


class DAGCycle(Exception):
    """
    Raised when a DAG is defined with cycles.
    """
    def __init__(self):
        error_message = """
        Failed to process DAG because it contains cycles.
        """
        super().__init__(error_message)


class SpecValidationError(Exception):
    """
    Raised when failing to validate a spec
    """
    def __init__(self, errors, model, kwargs):
        self.errors = errors
        self.model = model
        self.kwargs = kwargs

    def __str__(self):
        n_errors = len(self.errors)

        msg = (f'{n_errors} error{"" if n_errors == 1 else "s"} found '
               f'when validating {self.model.__name__} with values '
               f'{self.kwargs}\n\n'
               f'{display_errors(self.errors)}')

        return msg


class RemoteFileNotFound(Exception):
    """
    Raised by File clients when atempting to download a file that doesn't exist
    """
    pass


class MissingClientError(Exception):
    """
    Raised when failing to get a valid task-level or dag-level client
    for a Task or Product
    """
    pass


def display_errors(errors):
    return '\n'.join(f'{_display_error_loc(e)} ({e["msg"]})' for e in errors)


def _display_error_loc(error):
    return ' -> '.join(str(e) for e in error['loc'])
