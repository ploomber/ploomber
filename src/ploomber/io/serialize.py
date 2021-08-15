import json
import pickle
from pathlib import Path
from functools import wraps
from inspect import signature
from collections.abc import Mapping, Iterable

try:
    from joblib import dump as joblib_dump
except ModuleNotFoundError:
    joblib_dump = None

try:
    from cloudpickle import dump as cloudpickle_dump
except ModuleNotFoundError:
    cloudpickle_dump = None

from ploomber.products import MetaProduct


def _str2txt(obj, product):
    if not isinstance(obj, str):
        raise TypeError(f'Error serializing product {product!r} to .txt '
                        'with default serializer: data must be str, '
                        f'not {type(obj).__name__}')

    Path(product).write_text(obj)


def _obj2json(obj, product):
    try:
        serialized = json.dumps(obj)
    except TypeError:
        error = True
    else:
        error = False

    if error:
        raise TypeError(f'Error serializing product {product!r} to .json with '
                        'default serializer: Object of type  '
                        f'{type(obj).__name__} is not '
                        'JSON serializable')

    Path(product).write_text(serialized)


def _df2csv(obj, product):
    if not hasattr(obj, 'to_csv'):
        raise TypeError(f'Error serializing product {product!r} to .csv '
                        'with default serializer: expected a pandas.DataFrame '
                        f'but got an object of type {type(obj).__name__}')

    obj.to_csv(product)


def _df2parquet(obj, product):
    if not hasattr(obj, 'to_parquet'):
        raise TypeError(f'Error serializing product {product!r} to .parquet '
                        'with default serializer: expected a pandas.DataFrame '
                        f'but got an object of type {type(obj).__name__}')

    obj.to_parquet(product)


_EXTERNAL = {
    False: None,
    True: pickle.dump,
    'pickle': pickle.dump,
    'joblib': joblib_dump,
    'cloudpickle': cloudpickle_dump,
}

_DEFAULTS = {
    '.txt': _str2txt,
    '.json': _obj2json,
    '.csv': _df2csv,
    '.parquet': _df2parquet,
}


def _extension_mapping_validate(extension_mapping, fn):
    if extension_mapping is not None:
        if not isinstance(extension_mapping, Mapping):
            raise TypeError(
                f'Invalid extension_mapping {extension_mapping!r} for '
                f'decorated function {fn.__name__!r}. Expected '
                'it to be a dictionary but got a '
                f'{type(extension_mapping).__name__}')

        invalid_keys = {
            k
            for k in extension_mapping.keys() if not k.startswith('.')
        }

        if invalid_keys:
            raise ValueError(
                f'Invalid extension_mapping {extension_mapping!r} for '
                f'decorated function {fn.__name__!r}. Expected '
                'keys to start with a dot (e.g., ".csv"). Invalid '
                f'keys found: {invalid_keys!r}')


def _build_extension_mapping_final(extension_mapping, defaults, fn,
                                   defaults_provided, name):
    defaults_keys = set(defaults_provided)

    if defaults:
        if not isinstance(defaults, Iterable) or isinstance(defaults, str):
            raise TypeError(f'Invalid defaults {defaults!r} for '
                            f'decorated function {fn.__name__!r}. Expected '
                            'it to be a list but got a '
                            f'{type(defaults).__name__}')

        passed_defaults = set(defaults)

        if extension_mapping:
            overlap = passed_defaults & set(extension_mapping)
            if overlap:
                raise ValueError(
                    f'Error when adding @{name} decorator '
                    f'to function {fn.__name__!r}: '
                    'Keys in \'extension_mapping\' and \'defaults\' must not '
                    f'overlap (overlapping keys: {overlap})')

        unexpected_defaults = passed_defaults - defaults_keys

        if unexpected_defaults:
            raise ValueError(
                f'Error when adding @{name} decorator '
                f'to function {fn.__name__!r}: unexpected values in '
                '"defaults" argument. Valid values are: '
                f'{defaults_keys}. Unexpected '
                f'values: {unexpected_defaults}')

        defaults_map = {
            k: v
            for k, v in defaults_provided.items() if k in defaults
        }
        extension_mapping_final = {**defaults_map, **(extension_mapping or {})}
    else:
        extension_mapping_final = extension_mapping

    _extension_mapping_validate(extension_mapping_final, fn)

    return extension_mapping_final


def serializer(extension_mapping=None, *, fallback=False, defaults=None):
    """Decorator for serializing functions

    Parameters
    ----------
    extension_mapping : dict, default=None
        An extension -> function mapping. Calling the decorated function with a
        File of a given extension will use the one in the mapping if it exists,
        e.g., {'.csv': to_csv, '.json': to_json}.

    fallback : bool or str, default=False
        Determines what method to use if extension_mapping does not match the
        product to serialize. Valid values are True (uses the pickle module),
        'joblib', and 'cloudpickle'. If you use any of the last two, the
        corresponding moduel must be installed. If this is enabled, the
        body of the decorated function is never executed. To turn it off
        pass False.

    defaults : list, default=None
        Built-in serializing functions to use. Must be a list with any
        combinations of values: '.txt', '.json', '.csv', '.parquet'. To save
        to .txt, the returned object must be a string, for .json it must be
        a json serializable object (e.g., a list or a dict), for .csv and
        .parquet it must be a pandas.DataFrame. If using .parquet, a parquet
        library must be installed (e.g., pyarrow). If extension_mapping
        and defaults contain overlapping keys, an error is raised
    """
    def _serializer(fn):
        extension_mapping_final = _build_extension_mapping_final(
            extension_mapping, defaults, fn, _DEFAULTS, 'serializer')

        try:
            serializer_fallback = _EXTERNAL[fallback]
        except KeyError:
            error = True
        else:
            error = False

        if error:
            raise ValueError(f'Invalid fallback argument {fallback!r} '
                             f'in function {fn.__name__!r}. Must be one of '
                             "True, 'joblib', or 'cloudpickle'")

        if serializer_fallback is None and fallback in {
                'cloudpickle', 'joblib'
        }:
            raise ModuleNotFoundError(
                f'Error serializing with function {fn.__name__!r}. '
                f'{fallback} is not installed')

        n_params = len(signature(fn).parameters)
        if n_params != 2:
            raise TypeError(f'Expected serializer {fn.__name__!r} '
                            f'to take 2 arguments, but it takes {n_params!r}')

        @wraps(fn)
        def wrapper(obj, product):
            if isinstance(product, MetaProduct):
                _validate_obj(obj, product)

                for key, value in obj.items():
                    _serialize_product(value, product[key],
                                       extension_mapping_final, fallback,
                                       serializer_fallback, fn)
            else:
                _serialize_product(obj, product, extension_mapping_final,
                                   fallback, serializer_fallback, fn)

        return wrapper

    return _serializer


@serializer(fallback=True)
def serializer_pickle(obj, product):
    """A serializer that pickles everything
    """
    # this should never execute
    raise RuntimeError('Error when serializing with pickle module')


def _validate_obj(obj, product):
    if not isinstance(obj, Mapping):
        raise TypeError('Error serializing task: if task generates multiple '
                        f'products {product!r} the function must return '
                        f'a dictionary with the same keys (got {obj!r}, an '
                        f'object of type {type(obj).__name__}).')

    unexpected = set(obj) - set(product.products.products)
    missing = set(product.products.products) - set(obj)

    if missing or unexpected:
        error = ('Error serializing task: task generates products '
                 f'{product!r} but the function did not return a '
                 f'dictonary ({obj!r}) with valid keys. ')

        if missing:
            error += f'Missing keys: {missing!r}. '

        if unexpected:
            error += f'Unexpected keys: {unexpected!r}. '

        raise ValueError(error)


def _serialize_product(obj, product, extension_mapping, fallback,
                       serializer_fallback, fn):
    suffix = Path(product).suffix

    if extension_mapping and suffix in extension_mapping:
        extension_mapping[suffix](obj, product)
    elif fallback:
        with open(product, 'wb') as f:
            serializer_fallback(obj, f)
    else:
        fn(obj, product)
