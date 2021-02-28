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


class UpstreamKeyError(Exception):
    """
    Raised when trying to get an upstream dependency that does not exist,
    we have to implement a custom exception otherwise jinja is going
    to ignore our error messages (if we raise the usual KeyError).
    See: https://jinja.palletsprojects.com/en/2.11.x/templates/#variables
    """
    pass


class DAGSpecInitializationError(Exception):
    """
    Raised when failing to initialize a DAGSpec object
    """
    pass


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


def display_errors(errors):
    return '\n'.join(f'{_display_error_loc(e)} ({e["msg"]})' for e in errors)


def _display_error_loc(error):
    return ' -> '.join(str(e) for e in error['loc'])
