import importlib
import inspect
from pathlib import Path

import parso

from ploomber.sources.abc import Source
from ploomber.util.util import signature_check
from ploomber.util.dotted_path import load_dotted_path
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
                     f'{module!r} (module loaded from: {module_location!r})')


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

    Parameters
    ----------
    primitive : callable or str
        The function to use, an be a callable object or a dotted path string

    hot_reload : bool
        If True, continuously reloads the function to have the latest version

    needs_product : bool
        Pass True if the function needs a "product" parameter to be executed.
        This is used to provide appropriate error messages when the function's
        signature does not match this argument. The primary use for this
        parameter is InMemoryDAG, since functions used as task are
        not expected to have a "product" parameter
    """
    def __init__(self, primitive, hot_reload=False, needs_product=True):
        if not (callable(primitive) or isinstance(primitive, str)):
            raise TypeError(
                f'{type(self).__name__} must be initialized '
                f'with a Python callable or str, got: {primitive!r} '
                f'(type {type(primitive).__name__})')

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
        # TODO: we should be able to perform this validation from dotted
        # paths as well
        if not self._callable_loader.from_dotted_path:
            to_validate = set(params)
            fn_params = inspect.signature(self.primitive).parameters

            if not self._needs_product and 'product' in fn_params:
                raise TypeError(
                    f'Function {self.name!r} should not have '
                    'a \'product\' parameter, but return its result instead')

            # if source does not need product to be called and we got
            # a product parameter, remove it. NOTE: Task.render removes
            # product from params if dealing with an EmptyProduct - it's
            # better to always pass it and just remove it here to avoid
            # the second condition. i don't think there is any other
            # condition were we don't receive product here
            if not self._needs_product and 'product' in to_validate:
                to_validate.remove('product')

            signature_check(self.primitive, to_validate, self.name)

    def _post_init_validation(self, value):
        # TODO: verify the callable has a product parameter, if it's required
        pass

    @property
    def variables(self):
        raise NotImplementedError

    def extract_upstream(self):
        return PythonCallableExtractor(str(self)).extract_upstream()

    def extract_product(self):
        return PythonCallableExtractor(str(self)).extract_product()
