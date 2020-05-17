import importlib
import warnings
import inspect

from ploomber.sources.sources import Source
from ploomber.util.util import signature_check


class PythonCallableSource(Source):
    """
    A source object to encapsulate a Python callable (i.e. functions).
    """

    def __init__(self, primitive, hot_reload=False):
        if hot_reload:
            warnings.warn('hot_reload is not implement for '
                          'PythonCallableSource, this will be ignored')

        if not callable(primitive):
            raise TypeError('{} must be initialized'
                            'with a Python callable, got '
                            '"{}"'
                            .format(type(self).__name__),
                            type(primitive).__name__)

        self._primitive = primitive
        self._source_as_str = None
        self._loc = None
        self._hot_reload = hot_reload
        self.__source_lineno = None

    @property
    def primitive(self):
        if self._hot_reload:
            module = inspect.getmodule(self._primitive)
            name = self._primitive.__name__
            module_reloaded = importlib.reload(module)
            self._primitive = getattr(module_reloaded, name)

        return self._primitive

    @property
    def _source_lineno(self):
        if self.__source_lineno is None or self._hot_reload:
            _, self.__source_lineno = inspect.getsourcelines(self.primitive)

        return self.__source_lineno

    def __str__(self):
        if self._source_as_str is None or self._hot_reload:
            self._source_as_str = inspect.getsource(self.primitive)

        return self._source_as_str

    @property
    def doc(self):
        return self.primitive.__doc__

    @property
    def loc(self):
        if self._loc is None or self._hot_reload:
            self._loc = inspect.getsourcefile(self.primitive)

        return '{}:{}'.format(self._loc, self._source_lineno)

    def render(self, params):
        self._post_render_validation(None, params)

    @property
    def extension(self):
        return 'py'

    @property
    def name(self):
        return self.primitive.__name__

    def _post_render_validation(self, rendered_value, params):
        """
        Validation function executed after rendering
        """
        signature_check(self.primitive, params, self.name)

    def _post_init_validation(self, value):
        # TODO: verify the callable has a product parameter
        pass

    @property
    def variables(self):
        raise NotImplementedError
