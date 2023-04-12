import abc


class AbstractPlaceholder(abc.ABC):
    """
    Placeholder are objects that can eventually be converted to strings using
    str(placeholder), if this operator is used before they are fully resolved
    (e.g. they need to render first), they will raise an error.

    repr(placeholder) is always safe to use and it will return the rendered
    version if available, otherwise, it will show the raw string with
    tags, the representation is shortened if needed.

    Placeholders are mostly used inside Source objects, but are sometims
    also used to give Products placeholding features (e.g.
    by directly using Placeholder or SQLRelationPlaceholder)
    """

    @abc.abstractmethod
    def __str__(self):
        pass  # pragma: no cover

    @abc.abstractmethod
    def __repr__(self):
        pass  # pragma: no cover

    @abc.abstractmethod
    def render(self, params):
        pass  # pragma: no cover
