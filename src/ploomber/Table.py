"""
A mapping object with text and HTML representations
"""
from textwrap import TextWrapper
from copy import deepcopy
from pathlib import Path
import tempfile
from collections.abc import Mapping, Iterable
from tabulate import tabulate


def _is_iterable(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, str)


class Row:
    """A class to represent a dictionary as a table row

    Parameters
    ----------
    mapping
        Maps column names to a single value

    Examples
    --------
    >>> from ploomber.Table import Row
    >>> row = Row({'a': 'some value', 'b': 'another value'})
    >>> row # returns a table representation
    """
    def __init__(self, mapping):
        if not isinstance(mapping, Mapping):
            raise TypeError('Rows must be initialized with mappings')

        self._set_mapping(mapping)

    def __str__(self):
        return self._str

    def __repr__(self):
        return str(self)

    def _repr_html_(self):
        return self._html

    def __getitem__(self, key):
        if _is_iterable(key):
            return Row({k: self._mapping[k] for k in key})
        else:
            return self._mapping[key]

    def __setitem__(self, key, value):
        self._mapping[key] = value

    def __eq__(self, other):
        return self._mapping == other

    @property
    def columns(self):
        return tuple(self._mapping.keys())

    def _set_mapping(self, mapping):
        self._mapping = mapping
        self._str = tabulate([self._mapping],
                             headers='keys',
                             tablefmt='simple')
        self._html = tabulate([self._mapping], headers='keys', tablefmt='html')

    def _wrap_with(self, wrapper, exclude=None):
        exclude = exclude or []

        wrapped = {
            k: v if k in exclude else wrapper.fill(str(v))
            for k, v in self._mapping.items()
        }
        self._set_mapping(wrapped)


class Table:
    """A collection of rows

    Parameters
    ----------
    rows
        List of Row objects
    column_width
        Maximum column width, if None, no trimming happens, otherwise values
        are converted to string and trimmed
    """
    # Columns to exclude from wrapping
    EXCLUDE_WRAP = None

    def __init__(self, rows, column_width=20):
        if column_width:
            wrapper = TextWrapper(width=column_width,
                                  break_long_words=True,
                                  break_on_hyphens=True)
            for row in rows:
                row._wrap_with(wrapper, exclude=self.EXCLUDE_WRAP)

        self.column_width = column_width
        # TODO: remove this, only use ._values
        self._rows = self.data_preprocessing(rows)
        self._values = self._transform(rows)
        self._str = tabulate(self.values, headers='keys', tablefmt='simple')
        self._html = tabulate(self.values, headers='keys', tablefmt='html')

    def _transform(self, rows):
        """Convert [{key: value}, {key: value2}] to [{key: [value, value2]}]
        """
        if not len(rows):
            return {}

        cols_combinations = set(tuple(sorted(row.columns)) for row in rows)

        if len(cols_combinations) > 1:
            raise KeyError('All rows should have the same columns, got: '
                           '{}'.format(cols_combinations))

        columns = rows[0].columns

        return {col: [row[col] for row in rows] for col in columns}

    def __str__(self):
        return self._str

    def __repr__(self):
        return str(self)

    def _repr_html_(self):
        return self._html

    def __getitem__(self, key):
        if _is_iterable(key):
            return Table([row[key] for row in self._rows],
                         column_width=self.column_width)
        else:
            return self.values[key]

    def __iter__(self):
        for col in self.values:
            yield col

    def __len__(self):
        return len(self._values.keys())

    def __eq__(self, other):
        return self._rows == other

    def data_preprocessing(self, data):
        return data

    def save(self, path=None):
        if path is None:
            path = Path(tempfile.mktemp(suffix='.html'))

        path.write_text(self._html)

        return path

    def to_format(self, fmt):
        return tabulate(self.values, headers='keys', tablefmt=fmt)

    def to_pandas(self):
        import pandas as pd
        return pd.DataFrame(self.values)

    def to_dict(self):
        return deepcopy(self.values)

    @property
    def values(self):
        return self._values


class TaskReport(Row):
    EXCLUDE_WRAP = ['Ran?', 'Elapsed (s)']

    @classmethod
    def with_data(cls, name, ran, elapsed):
        return cls({'name': name, 'Ran?': ran, 'Elapsed (s)': elapsed})

    @classmethod
    def empty_with_name(cls, name):
        return cls.with_data(name, False, 0)


class BuildReport(Table):
    """A Table that adds a columns for checking task elapsed time
    """
    EXCLUDE_WRAP = ['Ran?', 'Elapsed (s)']

    def data_preprocessing(self, rows):
        """Create a build report from several tasks
        """
        total = sum([row['Elapsed (s)'] or 0 for row in rows])

        def compute_pct(elapsed, total):
            if not elapsed:
                return 0
            else:
                return 100 * elapsed / total

        for row in rows:
            row['Percentage'] = compute_pct(row['Elapsed (s)'], total)

        return rows
