from ploomber_core.exceptions import BaseException


class DAGRenderError(Exception):
    """Raise when a dag fails to render

    Notes
    -----
    This is a special exception that should only be raised under specific
    circumstances in the DAG implementation. Review carefully since this
    exceptions signals special output formatting in the CLI (via the
    @cli_endpoint decorator)
    """

    def __init__(self, message):
        message = message + "\nNeed help? https://ploomber.io/community"
        super().__init__(message)


class DAGBuildError(Exception):
    """Raise when a dag fails to build

    Notes
    -----
    This is a special exception that should only be raised under specific
    circumstances in the DAG implementation. Review carefully since this
    exceptions signals special output formatting in the CLI (via the
    @cli_endpoint decorator)
    """

    def __init__(self, message):
        message = message + "\nNeed help? https://ploomber.io/community"
        super().__init__(message)


class DAGWithDuplicatedProducts(BaseException):
    """Raised when more than one task has the same product"""

    pass


class DAGBuildEarlyStop(Exception):
    """
    This is raised on purpose to signal that the DAG should not continue
    executing but is not considered a build error
    """

    pass


class TaskInitializationError(BaseException):
    """Raised when a task fails to initialize"""

    pass


class TaskBuildError(BaseException):
    """Raise when a task fails to build"""

    pass


class TaskRenderError(Exception):
    """Raise when a task fails to render"""

    pass


class RenderError(Exception):
    """Raise when a template fails to render"""

    pass


class SourceInitializationError(BaseException):
    """Raise when a source fails to initialize due to wrong parameters"""

    pass


class MissingParametersCellError(SourceInitializationError):
    """Raise when a script or notebook is missing the parameters cell"""

    pass


class CallbackSignatureError(Exception):
    """When a callback function does not have the right signature"""

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

        msg = (
            f'{n_errors} error{"" if n_errors == 1 else "s"} found '
            f"when validating {self.model.__name__} with values "
            f"{self.kwargs}\n\n"
            f"{display_errors(self.errors)}"
        )

        return msg


class SQLTaskBuildError(TaskBuildError):
    """
    Raised by SQLScript and SQLDump when the .execute method fails
    """

    def __init__(self, type_, source_code, original):
        self.type_ = type_
        self.source_code = source_code
        self.original = original
        error_message = (
            "An error occurred when executing "
            f"{type_.__name__} task with "
            f"source code:\n\n{source_code!r}\n"
        )
        super().__init__(error_message)


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


class ValidationError(BaseException):
    """Raised when failed to validate input data"""

    pass


class RawBaseException(BaseException):
    def get_message(self):
        return str(self)


class MissingKeysValidationError(ValidationError):
    """Raised when failed to validate input data because keys were missing"""

    def __init__(self, message, missing_keys):
        self.missing_keys = missing_keys
        super().__init__(message)


class NetworkException(BaseException):
    """Raised when failing to call remote APIs"""

    def __init__(self, error_message, code=None):
        self.code = code
        super().__init__(error_message)


class PlotException(RawBaseException):
    pass


class DebuggingFinished(TaskBuildError):
    """Raised when quitting a debugging session"""

    def __init__(self, task_name):
        error_message = f"Finished debugging session for task {task_name!r}"
        super().__init__(error_message)


def display_errors(errors):
    return "\n".join(f'{_display_error_loc(e)} ({e["msg"]})' for e in errors)


def _display_error_loc(error):
    return " -> ".join(str(e) for e in error["loc"])
