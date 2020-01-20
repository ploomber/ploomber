
class TaskBuildError(Exception):
    """Raise when a task fails to build
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
