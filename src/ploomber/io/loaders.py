"""
Mixins to implement Task.load()
"""
from pathlib import Path


def _file_load(product, key=None, **kwargs):
    import pandas as pd

    if key:
        path = str(product[key])
    else:
        path = str(product)

    _extension2function = {
        '.csv': pd.read_csv,
        '.parquet': pd.read_parquet,
    }

    extension = Path(str(path)).suffix

    fn = _extension2function.get(extension)

    if fn is None:
        raise NotImplementedError(
            f'Cannot load file {path!r}. Files with extension '
            f'{extension!r} are currently unsupported, load manually by '
            'calling the appropriate unserializer function')

    return fn(path, **kwargs)


class FileLoaderMixin:
    """
    Load product as a pandas.DataFrame. Only .csv and .parquet files supported

    Parameters
    ----------
    key
        Key to use, if the task generates multiple products
    **kwargs
        Keyword arguments to pass to pandas.read_{csv, parquet} function
    """
    def load(self, key=None, **kwargs):
        return _file_load(self.product, key=key, **kwargs)
