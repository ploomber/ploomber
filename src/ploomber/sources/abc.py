import abc


class Source(abc.ABC):
    """Source abstract class

    Sources encapsulate the code that will be executed. Tasks only focus on
    execution and sources take care of the source code lifecycle: import,
    render (add parameter and validate) then hand off execution to the Task
    object.

    They validation logic is optional and is done at init time to prevent
    Tasks from being instantiated with ill-defined sources. For example, a
    SQLScript is expected to create a table/view, if the source does not
    contain a CREATE TABLE/VIEW statement, then validation could prevent this
    source code from executing. Validation can also happen after rendering.
    For example, a PythonCallableSource checks that the passed Task.params are
    compatible with the source function signature.

    A new Task does not mean a new source is required, the concrete classes
    implemented cover most use cases, if none of the current implementations
    matches their use case, they can either use a GenericSource or implement
    their own.

    Concrete classes should implement its own __repr__ method, which should
    display the source class, code (first few lines if long) and file location,
    if loaded from a file (sources can also load from strings)
    """
    @abc.abstractmethod
    def __init__(self, primitive, hot_reload=False):
        pass  # pragma: no cover

    @property
    @abc.abstractmethod
    def primitive(self):
        """
        Return the argument passed to build the source, unmodified. e.g. For
        SQL code this is a string, for notebooks it is a JSON string (or
        a plain text code string in a formate that can be converted to a
        notebook), for PythonCallableSource, it is a callable object.

        Should load from disk each time the user calls source.primitive
        if hot_reload is True. Any other object build from the primitive
        (e.g. the code with the injected parameters) should access this
        so hot_reload is propagated.
        """
        # FIXME: there are some API inconsistencies. Most soruces just
        # return the raw argument that initialized them but NotebookSource
        # loads the file if it's a path
        pass  # pragma: no cover

    # TODO: rename to params
    # NOTE: maybe allow dictionaries for cases where default values are
    # possible? python callables and notebooks
    @property
    @abc.abstractmethod
    def variables(self):
        pass  # pragma: no cover

    @abc.abstractmethod
    def render(self, params):
        """Render source (fill placeholders)

        If hot_reload is True, this should indirectly reload primitive
        from disk by using self.primitive
        """
        pass  # pragma: no cover

    # NOTE: maybe rename to path? the only case where it is not exactly a path
    # is when it is a callable (line number is added :line), but this is
    # intended as a human-readable property
    @property
    @abc.abstractmethod
    def loc(self):
        """
        Source location. For most cases, this is the path to the file that
        contains the source code. Used only for informative purpose (e.g.
        when doing dag.status())
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def __str__(self):
        """
        Must return the code that will be executed by the Task in a
        human-readable form (even if it's not the actual code sent to the
        task, the only case where this happens currently is for notebooks,
        human-readable would be plain text code but the actual code is an
        ipynb file in JSON), if it is modified by the render step
        (e.g. SQL code with tags), calling this before rendering should
        raise an error.

        This and the task params given an unambiguous definition of which code
        will be run. And it's actually the same code that will be executed
        for all cases except notebooks (where the JSON string is passed to
        the task to execute).
        """
        pass  # pragma: no cover

    @property
    @abc.abstractmethod
    def doc(self):
        """
        Returns code docstring
        """
        pass  # pragma: no cover

    # TODO: rename to suffix to be consistent with pathlib.Path
    @property
    @abc.abstractmethod
    def extension(self):
        """
        Optional property that should return the file extension, used for
        code normalization (applied before determining wheter two pieces
        or code are different). If None, no normalization is done.
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def _post_render_validation(self, rendered_value, params):
        """
        Validation function executed after rendering
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def _post_init_validation(self, value):
        pass  # pragma: no cover

    @property
    @abc.abstractmethod
    def name(self):
        """
        Name inferred from the primitive. Used to assign default values to
        Task.name
        """
        pass  # pragma: no cover

    # optional

    def extract_product(self):
        raise NotImplementedError('extract_product is not implemented in '
                                  '{}'.format(type(self).__name__))

    def extract_upstream(self):
        raise NotImplementedError('extract_upstream is not implemented in '
                                  '{}'.format(type(self).__name__))
