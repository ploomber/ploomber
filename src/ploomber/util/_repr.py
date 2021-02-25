def from_obj_and_arg_mapping(obj, arg_mapping):
    repr_ = ', '.join(f'{k}={v!r}' for k, v in arg_mapping.items())
    return '{}({})'.format(type(obj).__name__, repr_)
