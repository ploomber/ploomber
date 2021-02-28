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

from pydantic import BaseModel

from ploomber.util.util import call_dotted_path


class DottedPath(BaseModel):
    dotted_path: str

    class Config:
        extra = 'allow'


def call_spec(dotted_path_spec, raise_=True, reload=False):
    if isinstance(dotted_path_spec, str):
        dp = DottedPath(dotted_path=dotted_path_spec)
    elif isinstance(dotted_path_spec, Mapping):
        dp = DottedPath(**dotted_path_spec)
    else:
        raise TypeError('Expected dotted path spec to be a str or Mapping, '
                        f'got {dotted_path_spec!r} '
                        f'(type: {type(dotted_path_spec).__name__})')

    return call_dotted_path(dp.dotted_path,
                            kwargs=dp.dict(exclude={'dotted_path'}))
