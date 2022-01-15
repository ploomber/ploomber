"""
Utilities for dotted paths

Definitions:
    - dotted path:
        a string pointing to an object (e.g. my_module.my_function)
    - dotted path spec:
        a str or dict pointing to an object with optional parameters for
        calling it (a superset of the above)
"""
import warnings
from pathlib import Path
import importlib
from collections.abc import Mapping
from itertools import chain
from functools import reduce

import parso
import pydantic

from ploomber.exceptions import SpecValidationError


class DottedPath:
    """

    Parameters
    ----------
    dotted_path : str or dict
        A dotted path string such as module.function_name or a dictionary
        with a "dotted_path" key and any extra keys to be used as keyword
        arguments when calling the dotted path

    lazy_load : bool, default=False
        If True, defers dotted path loading until __call__ is executed

    allow_return_none : bool, default=True
        If True, it allows calling the dotted path to return None, otherwise
        it raises an exception
    """

    def __init__(self, dotted_path, lazy_load=False, allow_return_none=True):
        self._spec = DottedPathSpecModel.from_spec(dotted_path)
        self._callable = None
        self._allow_return_none = allow_return_none

        if not lazy_load:
            self._load_callable()

    @property
    def callable(self):
        return self._callable

    def _load_callable(self):
        self._callable = load_callable_dotted_path(self._spec.dotted_path)

    def __call__(self, *args, **kwargs):
        if self._callable is None:
            self._load_callable()

        spec_kwargs = self._spec.get_kwargs()
        overlap = set(spec_kwargs) & set(kwargs)

        if overlap:
            overlap_pretty = ", ".join(f"'{w}'" for w in overlap)
            warnings.warn('Got duplicated arguments '
                          f'({overlap_pretty}) when calling dotted path '
                          f'{self._spec.dotted_path!r}. Overriding values...')

        kwargs_final = {**spec_kwargs, **kwargs}

        # FIXME: this overlaps in functionality
        # with call_dotted_path so we must evaluate if we can replace
        # call_dotted_path with DottedPath
        out = self._callable(*args, **kwargs_final)

        if out is None and not self._allow_return_none:
            raise TypeError('Error calling dotted '
                            f'path {self._spec.dotted_path!r}. '
                            'Expected a value but got None')

        return out

    def __repr__(self):
        repr_ = f'{type(self).__name__}({self._spec.dotted_path!r})'

        if self._callable is not None:
            repr_ += f' (loaded: {self._callable})'

        return repr_


def _validate_dotted_path(dotted_path, raise_=True):
    parts = dotted_path.split('.')

    if len(parts) < 2 or not all(parts):
        if raise_:
            raise ValueError(f'Invalid dotted path {dotted_path!r}. '
                             'Value must be a dot separated '
                             'string, with at least two parts: '
                             '[module_name].[function_name]')
        else:
            return False

    return '.'.join(parts[:-1]), parts[-1]


def load_dotted_path(dotted_path, raise_=True, reload=False):
    """Load an object/function/module by passing a dotted path

    Parameters
    ----------
    dotted_path : str
        Dotted path to a module, e.g. ploomber.tasks.NotebookRunner
    raise_ : bool, default=True
        If True, an exception is raised if the module can't be imported,
        otherwise return None if that happens
    reload : bool, default=False
        Reloads the module after importing it
    """
    obj, module = None, None

    parsed = _validate_dotted_path(dotted_path, raise_=raise_)

    if parsed:
        mod, name = parsed
        main_mod = str(mod.split('.')[0])
        try:
            module = importlib.import_module(mod)
        except ModuleNotFoundError as e:
            if raise_:
                spec = importlib.util.find_spec(main_mod)

                msg = ('An error occured when trying to import dotted '
                       f'path {dotted_path!r}: {e}')

                if spec is not None:
                    msg = (msg +
                           f' (loaded {main_mod!r} from {spec.origin!r})')

                e.msg = msg

                raise

        if module:
            if reload:
                module = importlib.reload(module)

            try:
                obj = getattr(module, name)
            except AttributeError as e:
                if raise_:
                    e.args = ((f'Could not get {name!r} from module {mod!r} '
                               f'(loaded {mod!r} from {module.__file__!r}). '
                               'Ensure it is defined in such module'), )
                    raise
        return obj
    else:
        if raise_:
            raise ValueError(
                'Invalid dotted path value "{}", must be a dot separated '
                'string, with at least '
                '[module_name].[function_name]'.format(dotted_path))


def load_callable_dotted_path(dotted_path, raise_=True, reload=False):
    """
    Like load_dotted_path but verifies the loaded object is a callable
    """
    loaded_object = load_dotted_path(dotted_path=dotted_path,
                                     raise_=raise_,
                                     reload=reload)

    if not callable(loaded_object):
        raise TypeError(f'Error loading dotted path {dotted_path!r}. '
                        'Expected a callable object (i.e., some kind '
                        f'of function). Got {loaded_object!r} '
                        f'(an object of type: {type(loaded_object).__name__})')

    return loaded_object


def call_dotted_path(dotted_path, raise_=True, reload=False, kwargs=None):
    """
    Load dotted path (using load_callable_dotted_path), and call it with
    kwargs arguments, raises an exception if returns None

    Parameters
    ----------
    dotted_path : str
        Dotted path to call
    kwargs : dict, default=None
        Keyword arguments to call the dotted path
    """
    callable_ = load_callable_dotted_path(dotted_path=dotted_path,
                                          raise_=raise_,
                                          reload=reload)

    kwargs = kwargs or dict()

    try:
        out = callable_(**kwargs)
    except Exception as e:
        origin = locate_dotted_path(dotted_path).origin
        msg = str(e) + f' (Loaded from: {origin})'
        e.args = (msg, )
        raise

    if out is None:
        raise TypeError(f'Error calling dotted path {dotted_path!r}. '
                        'Expected a value but got None')

    return out


# TODO: this must be something like "locate_dotted_path_spec" since it's
# returning that
def locate_dotted_path(dotted_path):
    """
    Locates a dotted path, returns the spec for the module where the attribute
    is defined
    """
    tokens = dotted_path.split('.')
    module = '.'.join(tokens[:-1])
    spec = importlib.util.find_spec(module)

    if spec is None:
        raise ModuleNotFoundError(f'Module {module!r} does not exist')

    return spec


def locate_dotted_path_root(dotted_path):
    """
    Returns the module spec for a given dotted path.
    e.g. module.sub.another, checks that module exists
    """
    tokens = dotted_path.split('.')
    spec = importlib.util.find_spec(tokens[0])

    if spec is None:
        raise ModuleNotFoundError(f'Module {tokens[0]!r} does not exist')

    return spec


def _process_children(ch):
    if hasattr(ch, 'name'):
        return [(ch.name.value, ch.type, ch.get_code())]
    else:
        nested = ((c.get_defined_names(), c.type, c.get_code().strip())
                  for c in ch.children if hasattr(c, 'get_defined_names'))
        return ((name.value, type_, code) for names, type_, code in nested
                for name in names)


def _check_last_definition_is_function(module, name, dotted_path):
    children = [
        ch for ch in module.children[::-1]
        if hasattr(ch, 'name') or hasattr(ch, 'children')
    ]

    gen = chain(*(_process_children(ch) for ch in children))

    last_type = None
    code = None

    for name_, type_, code in gen:
        if name_ == name:
            last_type = type_
            last_code = code
            break

    if last_type and last_type != 'funcdef':
        raise TypeError(f'Failed to load dotted path {dotted_path!r}. '
                        f'Expected last defined {name!r} to be a function. '
                        f'Got:\n{last_code!r}')


def _check_defines_function_with_name(path, name, dotted_path):
    module = parso.parse(Path(path).read_text())

    # there could be multiple with the same name
    fns = [fn for fn in module.iter_funcdefs() if fn.name.value == name]

    if not fns:
        # check if there are import statements defining the attribute
        imports = [
            imp for imp in module.iter_imports()
            if name in [n.value for n in imp.get_defined_names()]
        ]

        if imports:
            # FIXME: show all imports in the error message instead of
            # only the first one
            import_ = imports[0].get_code()

            raise NotImplementedError(
                'Failed to locate dotted path '
                f'{dotted_path!r}, definitions from import statements are not '
                f'supported. Move the defitinion of function {name!r} '
                f'to {str(path)!r} and delete the import statement {import_!r}'
            )

        raise AttributeError(
            f'Failed to locate dotted path {dotted_path!r}. '
            f'Expected {str(path)!r} to define a function named {name!r}')

    # return the last definition to be consistent with inspect.getsourcefile
    fn_found = fns[-1]

    # if function is decorated, include decorators in the returned source code
    if fn_found.parent.type == 'decorated':
        fn_found = fn_found.parent

    _check_last_definition_is_function(module, name, dotted_path)

    return f'{path}:{fn_found.start_pos[0]}', fn_found.get_code().lstrip()


def lazily_locate_dotted_path(dotted_path):
    """
    Locates a dotted path, but unlike importlib.util.find_spec, it does not
    import submodules
    """
    _validate_dotted_path(dotted_path)
    parts = dotted_path.split('.')

    module_name = '.'.join(parts[:-1])
    first, middle, mod, symbol = parts[0], parts[1:-2], parts[-2], parts[-1]

    spec = importlib.util.find_spec(first)

    if spec is None:
        raise ModuleNotFoundError('Error processing dotted '
                                  f'path {dotted_path!r}, '
                                  f'no module named {first!r}')

    # python 3.6 returns 'namespace', python 3.7 an up returns None
    if spec.origin is None or spec.origin == 'namespace':
        raise ModuleNotFoundError('Error processing dotted '
                                  f'path {dotted_path!r}: '
                                  f'{first!r} appears to be a namespace '
                                  'package, which are not supported')

    origin = Path(spec.origin)
    location = origin.parent

    # a.b.c.d.e.f
    # a/__init__.py or a.py must exist
    # from b until d, there must be {name}/__init__.py
    # there must be e/__init__.py or e.py
    # f must be a symbol defined at e.py or e/__init__.py

    if len(parts) == 2:
        return _check_defines_function_with_name(origin, symbol, dotted_path)

    location = reduce(lambda x, y: x / y, [location] + middle)

    init = (location / mod / '__init__.py')
    file_ = (location / f'{mod}.py')

    if init.exists():
        return _check_defines_function_with_name(init, symbol, dotted_path)
    elif file_.exists():
        return _check_defines_function_with_name(file_, symbol, dotted_path)
    else:
        raise ModuleNotFoundError(f'No module named {module_name!r}. '
                                  f'Expected to find one of {str(init)!r} or '
                                  f'{str(file_)!r}, but none of those exist')


def dotted_path_exists(dotted_path):
    try:
        return lazily_locate_dotted_path(dotted_path)
    except (ModuleNotFoundError, AttributeError):
        return False


class BaseModel(pydantic.BaseModel):
    """Base model for specs
    """

    def __init__(self, **kwargs):
        # customize ValidationError message
        try:
            super().__init__(**kwargs)
        except pydantic.ValidationError as e:
            ex = e
        else:
            ex = None

        if ex:
            raise SpecValidationError(errors=ex.errors(),
                                      model=type(self),
                                      kwargs=kwargs)


class DottedPathSpecModel(BaseModel):
    dotted_path: str

    class Config:
        extra = 'allow'

    @classmethod
    def from_spec(cls, dotted_path_spec):
        if isinstance(dotted_path_spec, str):
            return cls(dotted_path=dotted_path_spec)
        elif isinstance(dotted_path_spec, Mapping):
            return cls(**dotted_path_spec)
        else:
            raise TypeError(
                'Expected dotted path spec to be a str or Mapping, '
                f'got {dotted_path_spec!r} '
                f'(type: {type(dotted_path_spec).__name__})')

    def get_kwargs(self):
        return self.dict(exclude={'dotted_path'})


def create_intermediate_modules(source_parts):
    """
    Creates the folder structure needed for a module specified
    by the parts of its name
    """
    dotted_path = '.'.join(source_parts)

    if dotted_path_exists(dotted_path):
        raise ValueError(f'Dotted path {dotted_path!r} already exists')

    *inner, last = source_parts

    spec = importlib.util.find_spec(inner[0])

    # .origin will be None for namespace packages
    # on Python 3.6, spec.origin is 'namespace' instead of None
    if spec and spec.origin is not None and spec.origin != 'namespace':
        inner[0] = Path(spec.origin).parent

    parent = Path(*inner)
    parent.mkdir(parents=True, exist_ok=True)

    for idx in range(len(inner)):
        init_file = Path(*inner[:idx + 1], "__init__.py")

        if not init_file.exists():
            init_file.touch()

    Path(parent, f"{last}.py").touch()
