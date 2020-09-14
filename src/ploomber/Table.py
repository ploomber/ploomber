"""
A mapping object with text and HTML representations
"""
from textwrap import TextWrapper
from copy import deepcopy
import shutil
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


class Table:
    """A collection of rows

    Parameters
    ----------
    rows
        List of Row objects
    column_width
        Maximum column width, if None, no trimming happens, otherwise values
        are converted to string and trimmed. If 'auto' width is determined
        based on terminal sized (using ``shutil.get_terminal_size()``)
    """
    # Columns to exclude from wrapping
    EXCLUDE_WRAP = None

    def __init__(self, rows, column_width='auto'):
        self.column_width = column_width

        if isinstance(rows, list):
            self._values = rows2columns(rows)
        else:
            self._values = rows

        self._values = self.data_preprocessing(self._values)

        self._str = None
        self._html = None

    def __str__(self):
        if self._str is None or self.column_width == 'auto':
            values = wrap_table_values(self._values, self.column_width,
                                       self.EXCLUDE_WRAP)

            self._str = tabulate(values, headers='keys', tablefmt='simple')

        return self._str

    def __repr__(self):
        return str(self)

    def _repr_html_(self):
        if self._html is None or self.column_width == 'auto':
            values = wrap_table_values(self._values, self.column_width,
                                       self.EXCLUDE_WRAP)

            self._html = tabulate(values, headers='keys', tablefmt='html')

        return self._html

    def __getitem__(self, key):
        if _is_iterable(key):
            return Table({k: v
                          for k, v in self.values.items() if k in key},
                         column_width=self.column_width)
        else:
            return self.values[key]

    def __iter__(self):
        for col in self.values:
            yield col

    def __len__(self):
        return len(self._values.keys())

    def __eq__(self, other):
        return self.values == other

    def data_preprocessing(self, values):
        return values

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
    EXCLUDE_WRAP = ['Ran?', 'Elapsed (s)', 'Percentage']

    def data_preprocessing(self, values):
        """Create a build report from several tasks
        """
        # in case the pipeline has no tasks...
        elapsed = values.get('Elapsed (s)', [])

        total = sum(elapsed)

        def compute_pct(elapsed, total):
            if not elapsed:
                return 0
            else:
                return 100 * elapsed / total

        values['Percentage'] = [compute_pct(r, total) for r in elapsed]

        return values


def rows2columns(rows):
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


def wrap_table_values(values, column_width, exclude):
    exclude = exclude or []

    if column_width is None:
        return values

    if column_width == 'auto':
        width_available = shutil.get_terminal_size().columns
        (exclude,
         column_width) = auto_determine_column_width(values, exclude,
                                                     width_available)

    wrapper = TextWrapper(width=column_width,
                          break_long_words=True,
                          break_on_hyphens=True)

    return wrap_mapping(values, wrapper, exclude=exclude)


def auto_determine_column_width(values, exclude, width_available):
    values_max_width = {
        k: max([len(str(value)) for value in v] + [len(k)])
        for k, v in values.items()
    }

    col_widths = {}

    # excluded columns should be left as is
    for k in exclude:
        # edge case: key might exist if the dag is empty and the excluded
        # column is addded in the Tabble.data_preprocessing, this happens
        # with the Ran? column in a build report
        if k in values_max_width:
            col_widths[k] = values_max_width.pop(k)

    def split_evenly(values, used):
        n_cols = len(values)

        if n_cols:
            # space available: total space - offset (size 2) between columns
            width = width_available - (n_cols - 1) * 2 - used
            return int(width / n_cols)
        else:
            return 0

    def space_required(values):
        n_cols = len(values)
        return sum(values.values()) + n_cols * 2

    # split remaining space evenly in the rest of the cols
    column_width = split_evenly(values_max_width, space_required(col_widths))

    # if any column requires less space than that, just give that max width
    short = [k for k, v in values_max_width.items() if v <= column_width]
    for k in short:
        col_widths[k] = values_max_width.pop(k)

    # NOTE: we could keep running the greedy algorithm to maximize space used
    # split the rest of the space evenly - the problem is that this is going
    # to increase runtime and highly favor short columns over large ones
    if values_max_width:
        column_width = split_evenly(values_max_width,
                                    space_required(col_widths))
    else:
        column_width = 0

    return list(col_widths), column_width


def wrap_mapping(mapping, wrapper, exclude=None):
    exclude = exclude or []

    wrapped = {
        k: v if k in exclude else wrap_value(v, wrapper)
        for k, v in mapping.items()
    }

    return wrapped


def wrap_value(value, wrapper):
    if isinstance(value, Iterable) and not isinstance(value, str):
        return [wrapper.fill(str(v)) for v in value]
    else:
        return wrapper.fill(str(value))
