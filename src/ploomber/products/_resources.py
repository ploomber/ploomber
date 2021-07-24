import hashlib
from pathlib import Path


def resolve_resources(params, relative_to):
    # params can be None
    if params is None:
        return None

    params_resolved = {}

    for key, value in params.items():
        if isinstance(key, str) and key.startswith('resource__'):
            params_resolved[key] = str(Path(relative_to, value).resolve())
        else:
            params_resolved[key] = value

    return params_resolved


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

    params_processed = {}

    for key, value in params.items():
        # TODO: make sure this wont cause issues with the CLI that also
        # uses __
        if isinstance(key, str) and key.startswith('resource__'):
            try:
                path = Path(value)
            except TypeError as e:
                raise TypeError(
                    f'Error reading params resource with key {key!r}. '
                    f'Expected value {value!r} to be a str, bytes '
                    f'or os.PathLike, not {type(value).__name__}') from e

            if not path.is_file():
                raise FileNotFoundError(
                    f'Error reading params resource with key {key!r}. '
                    f'Expected value {value!r} to be an existing file.')

            digest = hashlib.md5(path.read_bytes()).hexdigest()
            params_processed[key] = digest
        else:
            params_processed[key] = value

    return params_processed
