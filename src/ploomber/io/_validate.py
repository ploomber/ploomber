"""
Utilities for validating user input
"""
from ploomber.io import pretty_print


def is_in(value, options, name):
    if value not in options:
        raise ValueError(
            f"{value!r} is an invalid value for {name!r}. "
            f"Valid values: {pretty_print.iterable(options, repr_=True)}"
        )

    return value
