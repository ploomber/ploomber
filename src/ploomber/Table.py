"""
A mapping object with text and HTML representations
"""
from pathlib import Path
import tempfile
from collections.abc import Mapping, Iterable
from tabulate import tabulate


def _is_iterable(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, str)


class Row:
    """A class to represent a dictionary as a table row
    """

    def __init__(self, mapping):
        if not isinstance(mapping, Mapping):
            raise TypeError('Rows must be initialized with mappings')

        self._mapping = mapping
        self._str = tabulate([self._mapping], headers='keys',
                             tablefmt='simple')
        self._html = tabulate([self._mapping], headers='keys',
                              tablefmt='html')

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



class Table:
    """A collection of rows
    """

    def __init__(self, rows):
        self._rows = self.data_preprocessing(rows)
        self._rows_raw = [row._mapping for row in rows]
        self._str = tabulate(self._rows_raw, headers='keys', tablefmt='simple')
        self._html = tabulate(self._rows_raw, headers='keys', tablefmt='html')

    def __str__(self):
        return self._str

    def __repr__(self):
        return str(self)

    def _repr_html_(self):
        return self._html

    def __getitem__(self, key):
        if not _is_iterable(key):
            key = [key]

        return Table([row[key] for row in self._rows])

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
        return tabulate(self._rows_raw, headers='keys', tablefmt=fmt)


class BuildReport(Table):
    """A Table that adds a columns for checking task elapsed time
    """

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
