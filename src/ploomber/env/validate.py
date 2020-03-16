"""
Validation functions
"""
from collections.abc import Mapping


def raw_data_keys(d):
    """
    Validate raw dictionary, no top-level keys with leading underscores,
    except for _module, and no keys with double underscores anywhere
    """
    keys_all = get_keys_for_dict(d)
    errors = []

    try:
        no_double_underscores(keys_all)
    except ValueError as e:
        errors.append(str(e))

    try:
        no_leading_underscore(d.keys())
    except ValueError as e:
        errors.append(str(e))

    if errors:
        msg = 'Error validating env.\n' + '\n'.join(errors)
        raise ValueError(msg)


def get_keys_for_dict(d):
    """
    Get all (possibly nested) keys in a dictionary
    """
    out = []

    for k, v in d.items():
        out.append(k)

        if isinstance(v, Mapping):
            out += get_keys_for_dict(v)

    return out


def no_double_underscores(keys):
    not_allowed = [k for k in keys if '__' in k]

    if not_allowed:
        raise ValueError('Keys cannot have double '
                         'underscores, got: {}'.format(not_allowed))


def no_leading_underscore(keys):
    allowed = {'_module'}
    not_allowed = [k for k in keys if k.startswith('_') and k
                   not in allowed]

    if not_allowed:
        raise ValueError('Top-level keys cannot start '
                         'with an underscore, except for {}. '
                         'Got: {}'.format(allowed, not_allowed))
