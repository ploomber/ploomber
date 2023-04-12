from copy import deepcopy
import hashlib
from pathlib import Path
from collections.abc import Mapping
import os
import warnings

_KEY = "resources_"


def _cast_to_path(value, key):
    try:
        return Path(value)
    except TypeError as e:
        raise TypeError(
            f"Error reading params.resources_ with key {key!r}. "
            f"Expected value {value!r} to be a str, bytes "
            f"or os.PathLike, not {type(value).__name__}"
        ) from e


def _check_is_file(path, key):
    if not path.is_file():
        raise FileNotFoundError(
            f"Error reading params.resources_ with key {key!r}. "
            f"Expected value {str(path)!r} to be an existing file."
        )


def _check_file_size(path):
    resource_stat = os.stat(path)
    resource_file_size = resource_stat.st_size / 1e6
    if resource_file_size > 1:
        warnings.warn(
            f"resource_ {path!r} is {resource_file_size:.1f} MB. "
            "It is not recommended to use large files in "
            "resources_ since it increases task initialization time".format(
                path=path, resource_file_size=resource_file_size
            )
        )


def _validate(params):
    if not isinstance(params[_KEY], Mapping):
        raise TypeError(
            "Error reading params.resources_. 'resources_' must be a "
            "dictionary with paths to files to track, but got a value "
            f"{params[_KEY]} with type {type(params[_KEY]).__name__}"
        )


def resolve_resources(params, relative_to):
    # params can be None
    if params is None:
        return None

    if _KEY not in params:
        return deepcopy(params)

    _validate(params)

    resources = {}

    for key, value in params[_KEY].items():
        path = _cast_to_path(value, key)
        path_with_suffix = Path(relative_to, path)
        _check_is_file(path_with_suffix, key)
        resources[key] = str(path_with_suffix.resolve())

    params_out = deepcopy(params)
    params_out[_KEY] = resources

    return params_out


def process_resources(params):
    """
    Process resources in a parameters dict, computes the hash of the file for
    resources (i.e., params with the resource__ prefix)

    Parameters
    ----------
    params : dict
        Task parameters
    """

    # params can be None
    if params is None:
        return None

    if _KEY not in params:
        return deepcopy(params)

    _validate(params)

    resources = {}

    for key, value in params[_KEY].items():
        path = _cast_to_path(value, key)
        _check_is_file(path, key)
        _check_file_size(path)
        digest = hashlib.md5(path.read_bytes()).hexdigest()
        resources[key] = digest

    params_out = deepcopy(params)
    params_out[_KEY] = resources

    return params_out
