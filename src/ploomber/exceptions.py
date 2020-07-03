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
    Raised when failing to initialize a DAGSpec object from a dictionary
    """
    pass
