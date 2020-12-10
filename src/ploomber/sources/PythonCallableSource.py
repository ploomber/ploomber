import importlib
import inspect
from pathlib import Path

import parso

from ploomber.exceptions import TaskRenderError
from ploomber.sources.sources import Source
from ploomber.util.util import signature_check, load_dotted_path
from ploomber.static_analysis.python import PythonCallableExtractor


def find_code_for_fn_dotted_path(dotted_path):
    """
    Returns the source code for a function given the dotted path without
    importting it
    """
    tokens = dotted_path.split('.')
    module, name = '.'.join(tokens[:-1]), tokens[-1]
    spec = importlib.util.find_spec(module)

    if spec is None:
        raise ModuleNotFoundError('Error processing dotted '
                                  f'path {dotted_path!r}, '
                                  f'there is no module {module!r}')

    module_location = spec.origin

    module_code = Path(module_location).read_text()
    m = parso.parse(module_code)

    for f in m.iter_funcdefs():
        if f.name.value == name:
            # remove leading whitespace that parso keeps, which is not
            # what inspect.getsource does - this way we get the same code
            return f.get_code().lstrip()

    raise ValueError(f'Error processing dotted path {dotted_path!r}, '
                     f'there is no function named {name!r} in module '
                     f'{module!r}')


class CallableLoader:
    """
    If initialized with a string, it provies some functionality
    without importing it.
    """

    # TODO: this class could be used anywhere where we have to load callables
    # from dotted paths in the spec (and could also be useful for the Python
    # API): sources, hooks and clients. We can add a "lazy" option that allows
    # to keep a reference to the callable and import it until it's called
    def __init__(self, primitive, hot_reload):
        self.hot_reload = hot_reload
        self._from_dotted_path = isinstance(primitive, str)
        self._primitive = primitive

        if self.hot_reload and not self._from_dotted_path:
            self.module_name = inspect.getmodule(primitive).__name__
            self.fn_name = primitive.__name__

            # if using hot reload, we cannot keep the reference to the
            # original function, otherwise pickle will give errors
            self._primitive = None
        elif self.hot_reload and self._from_dotted_path:
            raise NotImplementedError('hot_reload is not implemented when '
                                      'initializing from a dotted path')

    def load(self):
        if self._from_dotted_path:
            return load_dotted_path(self._primitive)
        else:
            if self.hot_reload:
                module = importlib.import_module(self.module_name)
                importlib.reload(module)
                return getattr(module, self.fn_name)
            else:
                return self._primitive

    def get_source(self):
        if self._from_dotted_path:
            return find_code_for_fn_dotted_path(self._primitive)
        else:
            return inspect.getsource(self.load())

    @property
    def from_dotted_path(self):
        return self._from_dotted_path

    @property
    def name(self):
        if self._from_dotted_path:
            return self._primitive.split('.')[-1]
        else:
            return self.load().__name__


class PythonCallableSource(Source):
    """
    A source object to encapsulate a Python callable (i.e. functions).
    """
    def __init__(self, primitive, hot_reload=False, needs_product=True):
        if not (callable(primitive) or isinstance(primitive, str)):
            raise TypeError('{} must be initialized'
                            'with a Python callable or str, got '
                            '"{}"'.format(
                                type(self).__name__,
                                type(primitive).__name__))
        self._callable_loader = CallableLoader(primitive, hot_reload)
        self._source_as_str = None
        self._loc = None
        self._hot_reload = hot_reload
        self._needs_product = needs_product
        self.__source_lineno = None

    @property
    def primitive(self):
        return self._callable_loader.load()

    @property
    def _source_lineno(self):
        if self.__source_lineno is None or self._hot_reload:
            _, self.__source_lineno = inspect.getsourcelines(self.primitive)

        return self.__source_lineno

    def __repr__(self):
        return "{}({}) (defined at: '{}')".format(
            type(self).__name__, self.name, self.loc)

    def __str__(self):
        if self._source_as_str is None or self._hot_reload:
            self._source_as_str = self._callable_loader.get_source()

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
        return self._callable_loader.name

    def _post_render_validation(self, rendered_value, params):
        """
        Validation function executed after rendering
        """
        if not self._callable_loader.from_dotted_path:
            to_validate = set(params)

            if not self._needs_product:
                to_validate.remove('product')

            # TODO: provide a better error message when task does not need
            # product but the function has it, the current error is confusing
            # because we remove "product" from the params and then pass
            # validate
            signature_check(self.primitive, to_validate, self.name)

    def _post_init_validation(self, value):
        # TODO: verify the callable has a product parameter
        pass

    @property
    def variables(self):
        raise NotImplementedError

    def extract_upstream(self):
        return PythonCallableExtractor(str(self)).extract_upstream()

    def extract_product(self):
        return PythonCallableExtractor(str(self)).extract_product()
