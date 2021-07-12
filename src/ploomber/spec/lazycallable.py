from ploomber.util import dotted_path
from ploomber.util.util import callback_check


# TODO: remove and merge with DottedPathSpec, we are actually not
# using this and its purpose is the same as DottedPathSpec
class LazyCallable:
    """A callable that can be lazily imported from a dotted path

    Parameters
    ----------
    primitive : str or callable
        Dotted path sting or callable. If str, the dotted path is imported
        until called for the first time
    """
    def __init__(self, primitive):
        self._primitive = primitive
        self._callable = None

    def __call__(self, *args, **kwargs):
        callable_ = self._get_callable()
        return callable_(*args, **kwargs)

    def call_with_available(self, accepted_kwargs):
        """
        Check the signature of the callable to ensure it has the passed
        keyword arguments. It causes to import the dotted path, if it
        hasn't been done yet. Then it calls the function.
        """
        callable_ = self._get_callable()
        kwargs_to_use = callback_check(callable_, accepted_kwargs)
        return self(**kwargs_to_use)

    def _get_callable(self):
        if self._callable is None:
            if isinstance(self._primitive, str):
                self._callable = dotted_path.load_callable_dotted_path(
                    self._primitive)
            else:
                self._callable = self._primitive

        return self._callable

    def __repr__(self):
        return f'{type(self).__name__}({self._primitive!r})'
