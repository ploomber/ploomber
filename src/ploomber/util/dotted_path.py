"""
Utilities for dotted paths

Definitions:
    - dotted path:
        a string pointing to an object (e.g. my_module.my_function)
    - dotted path spec:
        a str or dict pointing to an object with optional parameters for
        calling it (a superset of the above)
"""
from collections.abc import Mapping

import pydantic

from ploomber.util.util import call_dotted_path
from ploomber.exceptions import SpecValidationError


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
            errors = ex.errors()
            no_errors = len(errors)

            msg = (
                f'{no_errors} error{"" if no_errors == 1 else "s"} found '
                f'when validating {ex.model.__name__} with values {kwargs}\n\n'
                f'{display_errors(errors)}')

            raise SpecValidationError(msg, errors)


def display_errors(errors):
    return '\n'.join(f'{_display_error_loc(e)} ({e["msg"]})' for e in errors)


def _display_error_loc(error):
    return ' -> '.join(str(e) for e in error['loc'])


class DottedPathSpec(BaseModel):
    dotted_path: str

    class Config:
        extra = 'allow'


def call_spec(dotted_path_spec, raise_=True, reload=False):
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
