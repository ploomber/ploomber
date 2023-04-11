import json
import pickle
from pathlib import Path
from functools import wraps
from inspect import signature

try:
    from joblib import load as joblib_load
except ModuleNotFoundError:
    joblib_load = None

try:
    from cloudpickle import load as cloudpickle_load
except ModuleNotFoundError:
    cloudpickle_load = None
try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None

from ploomber.products import MetaProduct
from ploomber.io.serialize import _build_extension_mapping_final

_EXTERNAL = {
    False: None,
    True: pickle.load,
    "pickle": pickle.load,
    "joblib": joblib_load,
    "cloudpickle": cloudpickle_load,
}


def _txt2str(product):
    return Path(product).read_text()


def _json2obj(product):
    return json.loads(Path(product).read_text())


def _csv2df(product):
    if pd is None:
        raise ModuleNotFoundError(
            "Error using .csv default unserializer: " "pandas is not installed"
        )

    return pd.read_csv(product)


def _parquet2df(product):
    if pd is None:
        raise ModuleNotFoundError(
            "Error using .parquet default unserializer: " "pandas is not installed"
        )

    return pd.read_parquet(product)


_DEFAULTS = {
    ".txt": _txt2str,
    ".json": _json2obj,
    ".csv": _csv2df,
    ".parquet": _parquet2df,
}


def unserializer(
    extension_mapping=None, *, fallback=False, defaults=None, unpack=False
):
    """Decorator for unserializing functions

    Parameters
    ----------
    extension_mapping : dict, default=None
        An extension -> function mapping. Calling the decorated function with a
        File of a given extension will use the one in the mapping if it exists,
        e.g., {'.csv': from_csv, '.json': from_json}.

    fallback : bool or str, default=False
        Determines what method to use if extension_mapping does not match the
        product to unserialize. Valid values are True (uses the pickle module),
        'joblib', and 'cloudpickle'. If you use any of the last two, the
        corresponding moduel must be installed. If this is enabled, the
        body of the decorated function is never executed. To turn it off
        pass False.

    defaults : list, default=None
        Built-in unserializing functions to use. Must be a list with any
        combinations of values: '.txt', '.json', '.csv', '.parquet'.
        Unserializing .txt, returns a string, for .json returns any
        JSON-unserializable object (e.g., a list or a dict), .csv and
        .parquet return a pandas.DataFrame. If using .parquet, a parquet
        library must be installed (e.g., pyarrow). If extension_mapping
        and defaults contain overlapping keys, an error is raises

    unpack : bool, default=False
        If True and the task product points to a directory, it will call
        the unserializer one time per file in the directory. The unserialized
        object will be a dictionary where keys are the filenames and values
        are the unserialized objects. Note that this isn't recursive, it only
        looks at files that are immediate children of the product directory.
    """

    def _unserializer(fn):
        extension_mapping_final = _build_extension_mapping_final(
            extension_mapping, defaults, fn, _DEFAULTS, "unserializer"
        )

        try:
            unserializer_fallback = _EXTERNAL[fallback]
        except KeyError:
            error = True
        else:
            error = False

        if error:
            raise ValueError(
                f"Invalid fallback argument {fallback!r} "
                f"in function {fn.__name__!r}. Must be one of "
                "True, 'joblib', or 'cloudpickle'"
            )

        if unserializer_fallback is None and fallback in {"cloudpickle", "joblib"}:
            raise ModuleNotFoundError(
                f"Error unserializing with function {fn.__name__!r}. "
                f"{fallback} is not installed"
            )

        n_params = len(signature(fn).parameters)
        if n_params != 1:
            raise TypeError(
                f"Expected unserializer {fn.__name__!r} "
                f"to take 1 argument, but it takes {n_params!r}"
            )

        @wraps(fn)
        def wrapper(product):
            if isinstance(product, MetaProduct):
                return {
                    key: _unserialize_product(
                        value,
                        extension_mapping_final,
                        fallback,
                        unserializer_fallback,
                        fn,
                        unpack,
                    )
                    for key, value in product.products.products.items()
                }

            else:
                return _unserialize_product(
                    product,
                    extension_mapping_final,
                    fallback,
                    unserializer_fallback,
                    fn,
                    unpack,
                )

        return wrapper

    return _unserializer


@unserializer(fallback=True)
def unserializer_pickle(product):
    """An unserializer that unpickles everything"""
    # this should never execute
    raise RuntimeError("Error when unserializing with pickle module")


def _make_unserializer(fn):
    def _unserialize(product):
        with open(product, "rb") as f:
            obj = fn(f)

        return obj

    return _unserialize


def _unserialize_product(
    product, extension_mapping, fallback, unserializer_fallback, fn, unpack
):
    if unpack and Path(product).is_dir():
        out = {}

        for path in Path(product).glob("*"):
            unserializer = _determine_unserializer(
                path, extension_mapping, fallback, unserializer_fallback, fn
            )

            out[path.name] = unserializer(path)

        return out

    # treat product as a single file...
    else:
        unserializer = _determine_unserializer(
            product, extension_mapping, fallback, unserializer_fallback, fn
        )
        return unserializer(product)


def _determine_unserializer(
    product, extension_mapping, fallback, unserializer_fallback, fn
):
    suffix = Path(product).suffix

    if extension_mapping and suffix in extension_mapping:
        return extension_mapping[suffix]
    elif fallback:
        unserializer = _make_unserializer(unserializer_fallback)
        return unserializer
    else:
        return fn
