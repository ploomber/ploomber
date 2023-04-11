"""
A mapping object with text and HTML representations
"""
from functools import reduce
from warnings import warn
from textwrap import TextWrapper
from copy import deepcopy
import shutil
from collections.abc import Mapping, Iterable
from tabulate import tabulate

from ploomber.util.util import isiterable_not_str

_BETWEEN_COLUMN_WIDTH = 2


class Row:
    """A class to represent a dictionary as a table row

    Parameters
    ----------
    mapping
        Maps column names to a single value

    Examples
    --------
    >>> from ploomber.table import Row
    >>> row = Row({'a': 'some value', 'b': 'another value'})
    """

    def __init__(self, mapping):
        if not isinstance(mapping, Mapping):
            raise TypeError("Rows must be initialized with mappings")

        self._set_mapping(mapping)

    def __str__(self):
        return self._str

    def __repr__(self):
        return str(self)

    def _repr_html_(self):
        return self._html

    def __getitem__(self, key):
        if isiterable_not_str(key):
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
        self._str = tabulate([self._mapping], headers="keys", tablefmt="simple")
        self._html = tabulate([self._mapping], headers="keys", tablefmt="html")


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

    def __init__(self, rows, column_width="auto"):
        self.column_width = column_width

        if isinstance(rows, list):
            self._values = rows2columns(rows)
        else:
            self._values = rows

        self._values = self.data_preprocessing(self._values)

        self._str = None
        self._html = None

    def __str__(self):
        if self._str is None or self.column_width == "auto":
            values = wrap_table_dict(self._values, self.column_width, self.EXCLUDE_WRAP)
            self._str = tabulate(values, headers="keys", tablefmt="simple")

        return self._str

    def __repr__(self):
        return str(self)

    def _repr_html_(self):
        if self._html is None or self.column_width == "auto":
            values = wrap_table_dict(self._values, self.column_width, self.EXCLUDE_WRAP)

            self._html = tabulate(values, headers="keys", tablefmt="html")

        return self._html

    def __getitem__(self, key):
        if isiterable_not_str(key):
            return Table(
                {k: v for k, v in self.values.items() if k in key},
                column_width=self.column_width,
            )
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

    def to_format(self, fmt):
        values = wrap_table_dict(self.values, self.column_width, self.EXCLUDE_WRAP)
        return tabulate(values, headers="keys", tablefmt=fmt)

    def to_pandas(self):
        import pandas as pd

        return pd.DataFrame(self.values)

    def to_dict(self):
        return deepcopy(self.values)

    @property
    def values(self):
        return self._values

    @classmethod
    def from_dicts(cls, dicts, complete_keys=False):
        if complete_keys:
            keys_all = reduce(lambda a, b: set(a) | set(b), dicts)
            default = {k: "" for k in keys_all}
            return cls([Row({**default, **d}) for d in dicts])
        else:
            return cls([Row(d) for d in dicts])


class TaskReport(Row):
    EXCLUDE_WRAP = ["Ran?", "Elapsed (s)"]

    @classmethod
    def with_data(cls, name, ran, elapsed):
        return cls({"name": name, "Ran?": ran, "Elapsed (s)": elapsed})

    @classmethod
    def empty_with_name(cls, name):
        return cls.with_data(name, False, 0)


class BuildReport(Table):
    """A Table that adds a columns for checking task elapsed time"""

    EXCLUDE_WRAP = ["Ran?", "Elapsed (s)", "Percentage"]

    def data_preprocessing(self, values):
        """Create a build report from several tasks"""
        # in case the pipeline has no tasks...
        elapsed = values.get("Elapsed (s)", [])

        total = sum(elapsed)

        def compute_pct(elapsed, total):
            if not elapsed:
                return 0
            else:
                return 100 * elapsed / total

        values["Percentage"] = [compute_pct(r, total) for r in elapsed]

        return values


def rows2columns(rows):
    """Convert [{key: value}, {key: value2}] to [{key: [value, value2]}]"""
    if not len(rows):
        return {}

    cols_combinations = set(tuple(sorted(row.columns)) for row in rows)

    if len(cols_combinations) > 1:
        raise KeyError(
            "All rows should have the same columns, got: "
            "{}".format(cols_combinations)
        )

    columns = rows[0].columns

    return {col: [row[col] for row in rows] for col in columns}


def wrap_table_dict(table_dict, column_width, exclude):
    """Wraps a columns to take at most column_width characters

    Parameters
    ----------
    column_width : int, 'auto' or None
        Width per column. Splits evenly if 'auto', does not wrap if None
    exclude : list
        Exclude columns from wrapping (show them in a single line)
    """
    exclude = exclude or []

    if column_width is None:
        return table_dict

    if column_width == "auto":
        column_width = calculate_wrapping(
            table_dict,
            do_not_wrap=exclude,
            width_total=shutil.get_terminal_size().columns,
        )

    # NOTE: the output of this algorithm may return a table that does not use
    # between 0 and {column - 1} characters. We could always take all the
    # space available if we refactor and do not keep column_width fixed for
    # all columns
    wrapper = TextWrapper(
        width=column_width, break_long_words=True, break_on_hyphens=True
    )

    return apply_wrapping(table_dict, wrapper, exclude=exclude)


def separator_width(header_length, max_value_length):
    """
    Calculates the width of the '---' line that separates header from content
    """
    n_value_extra = header_length - max_value_length

    if n_value_extra >= -2:
        return header_length + 2
    else:
        return max_value_length


def width_required_for_column(header, values):
    """
    Spaced needed to display column in a single line, accounts for the two
    extra characters that the tabulate package adds to the header when the
    content is too short
    """
    values_max = -1 if not values else max(len(str(v)) for v in values)
    return max(values_max, separator_width(len(header), values_max))


def calculate_wrapping(table_dict, do_not_wrap, width_total):
    """
    Determines the column width by keeping some columns unwrapped (show all
    rows, including the header in a single line) and distributing the
    remaining space evenly. Accounts for the betwee-column spacing.
    """
    # space required to display a given column on a single column
    width_required = {
        header: width_required_for_column(header, values)
        for header, values in table_dict.items()
    }

    # TODO: pass set(table_dict) instead of table_dict
    column_width = _calculate_wrapping(
        table_dict, do_not_wrap, width_total, width_required
    )

    return column_width


def _calculate_wrapping(table_dict, do_not_wrap, width_total, width_required):
    width_offset = 0

    # how much space we are already taking by not wrapping columns in
    # do_not_wrap
    for col_name in do_not_wrap:
        # edge case: key might not exist if the dag is empty col_name
        # is added during Tabble.data_preprocessing (e.g.,) the "Ran?"" column
        # in a build report
        if col_name in width_required:
            width_offset += width_required[col_name]

    # how much we have left - takes into account the bweteen-column spacing
    # of two characters
    width_remaining = (
        width_total - width_offset - len(do_not_wrap) * _BETWEEN_COLUMN_WIDTH
    )

    cols_to_wrap = set(table_dict) - set(do_not_wrap)

    # there should be at least one column to wrap to continue
    if not cols_to_wrap:
        return width_total

    # split remaining space evenly in the rest of the cols
    column_width = equal_column_width(
        n_cols=len(cols_to_wrap), width_total=width_remaining
    )

    # check if we have short columns. this means we can give more space
    # to the others
    short = {col for col in cols_to_wrap if width_required[col] <= column_width}

    # if there is a (strict) subset columns that can display in a single line
    # with less than column_width, call again. When short has all columns
    # already, just return whatever number we have, there are no more columns
    # to distribute space
    if short and short < set(table_dict):
        return _calculate_wrapping(
            table_dict,
            do_not_wrap=do_not_wrap + list(short),
            width_total=width_total,
            width_required=width_required,
        )
    else:
        return column_width


def equal_column_width(n_cols, width_total):
    """
    Max column width if splitting width_total equally among n_cols. Note
    that before computing column width, a quantity is substracted to account
    for required for spacing between columns
    """
    if not n_cols:
        raise ValueError("n_cols must be >0")

    offset = (n_cols - 1) * _BETWEEN_COLUMN_WIDTH
    width_remaining = width_total - offset
    width_column = int(width_remaining / n_cols)

    # degenerate case: not even a single space to display. Return width of
    # 1 but show a warning, since the table will be illegible
    if width_column < 1:
        warn(
            f"Not enough space to display {n_cols} columns with "
            f"a width of {width_total}. Using a column width of 1"
        )
        return 1

    return width_column


def apply_wrapping(table_dict, wrapper, exclude=None):
    """
    Wrap text using a wrapper, excluding columns in exclude
    """
    exclude = exclude or []

    return dict(
        apply_wrapping_to_column(header, values, exclude, wrapper)
        for header, values in table_dict.items()
    )


def apply_wrapping_to_column(header, values, exclude, wrapper):
    # TODO: test this
    if header in exclude:
        return header, values

    # wrapping values is simple, apply the wrapper directly
    values_wrapped = wrap_elementwise(values, wrapper)

    # wrapping the header has a detail: if there isn't enough space
    # for the 2 character offset that tabulate adds to the header, the
    # column will end up taking more space than expected, we don't want that.
    # wrap the header a bit more if necessary. Clip to 0 since this can be
    # negative with large headers
    offset = max(wrapper.width - len(header), 0)

    if offset >= 2:
        header_wrapped = wrap_elementwise(header, wrapper)
    else:
        _wrapper = TextWrapper(
            width=wrapper.width - (2 - offset),
            break_long_words=True,
            break_on_hyphens=True,
        )
        header_wrapped = wrap_elementwise(header, _wrapper)

    return header_wrapped, values_wrapped


def wrap_elementwise(value, wrapper):
    """Apply wrap if str (elementwise if iterable of str)"""
    if isinstance(value, Iterable) and not isinstance(value, str):
        return [wrapper.fill(str(v)) for v in value]
    else:
        return wrapper.fill(str(value))
