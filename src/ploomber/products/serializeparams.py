from copy import copy
import warnings
from ploomber.env.expand import iterate_nested_dict


def remove_non_serializable_top_keys(d):
    """
    Remove any parameters that contain unserializable objects so that
    products will still update based on their serializable parameters.

    Parameters
    ----------
    d: a dictionary containing parameters
    """
    def is_json_serializable(o):
        return isinstance(
            o, (dict, list, tuple, str, int, float, bool)) or o is None

    out = copy(d)
    for (_, _, current_val, preffix) in iterate_nested_dict(d):
        if not is_json_serializable(current_val):
            top_key = preffix[0]
            if top_key in out:
                del out[top_key]
            warnings.warn(f'Param {top_key!r} contains an unserializable '
                          f'object: {current_val!r}, it will be ignored. '
                          'Changes to it will not trigger task execution.')

    return out