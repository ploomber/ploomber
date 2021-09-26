from copy import copy
import warnings
from ploomber.env.expand import iterate_nested_dict


def is_json_serializable(obj):
    # https://github.com/python/cpython/blob/dea59cf88adf5d20812edda330e085a4695baba4/Lib/json/encoder.py#L73
    return isinstance(
        obj, (dict, list, tuple, str, int, float, bool)) or obj is None


def remove_non_serializable_top_keys(obj):
    """
    Remove top-level keys with unserializable objects, warning if necessary

    Parameters
    ----------
    d: a dictionary containing parameters
    """
    out = copy(obj)

    for (_, _, current_val, preffix) in iterate_nested_dict(obj):
        if not is_json_serializable(current_val):
            top_key = preffix[0]

            if top_key in out:
                del out[top_key]

            warnings.warn(f'Param {top_key!r} contains an unserializable '
                          f'object: {current_val!r}, it will be ignored. '
                          'Changes to it will not trigger task execution.')

    return out
