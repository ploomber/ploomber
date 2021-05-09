"""
Utilities for dotted paths

Definitions:
    - dotted path:
        a string pointing to an object (e.g. my_module.my_function)
    - dotted path spec:
        a str or dict pointing to an object with optional parameters for
        calling it (a superset of the above)
"""
import importlib
from collections.abc import Mapping

import pydantic

from ploomber.util.util import _parse_module
from ploomber.exceptions import SpecValidationError


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

    parsed = _parse_module(dotted_path, raise_=raise_)

    if parsed:
        mod, name = parsed

        try:
            module = importlib.import_module(mod)
        except ImportError as e:
            if raise_:
                # we want to raise ethe same error type but chaining exceptions
                # produces a long verbose output, so we just modify the
                # original message to add more context, it's ok to hide the
                # original traceback since it will just point to lines
                # in the importlib module, which isn't useful for the user
                e.msg = ('An error happened when trying to '
                         'import dotted path "{}": {}'.format(
                             dotted_path, str(e)))
                raise

        if module:
            if reload:
                module = importlib.reload(module)

            try:
                obj = getattr(module, name)
            except AttributeError as e:
                if raise_:
                    # same as in the comment above
                    e.args = (
                        'Could not get "{}" from module '
                        '"{}" (loaded from: {}), make sure it is a valid '
                        'callable defined in such module'.format(
                            name, mod, module.__file__), )
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
    Load dotted path (using load_callable_dotted_path), and call it without
    arguments, raises an exception if returns None
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


class DottedPathSpec(BaseModel):
    dotted_path: str

    class Config:
        extra = 'allow'


def call_spec(dotted_path_spec, raise_=True, reload=False):
    """Call a dotted path initialized from a spec (dictionary)
    """
    if isinstance(dotted_path_spec, str):
        dp = DottedPathSpec(dotted_path=dotted_path_spec)
    elif isinstance(dotted_path_spec, Mapping):
        dp = DottedPathSpec(**dotted_path_spec)
    else:
        raise TypeError('Expected dotted path spec to be a str or Mapping, '
                        f'got {dotted_path_spec!r} '
                        f'(type: {type(dotted_path_spec).__name__})')

    return call_dotted_path(dp.dotted_path,
                            kwargs=dp.dict(exclude={'dotted_path'}))
