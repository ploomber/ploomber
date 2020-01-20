"""
A mapping object with text and HTML representations
"""
from pathlib import Path
import tempfile
from collections.abc import Mapping
from tabulate import tabulate


class Row:
    """A class to represent a dictionary as a table row
    """
    def __init__(self, data):
        if not isinstance(data, Mapping):
            raise TypeError('Rows must be initialized with mappings')

        self._data = data
        self._repr = tabulate([self._data], headers='keys')
        self._html = tabulate([self._data], headers='keys', tablefmt='html')

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return self._repr

    def _repr_html_(self):
        return self._html

    def __getitem__(self, key):
        return self._data[key]


class Table:
    """A collection of rows

    >>> from ploomber.Table import Table
    >>> data = {'name': 'task', 'elapsed': 10, 'errors': False}
    >>> table = Table(data)
    >>> table
    """
    def __init__(self, data):
        self._data = self.data_preprocessing([d._data for d in data])
        self._repr = tabulate(self._data, headers='keys')
        self._html = tabulate(self._data, headers='keys', tablefmt='html')

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return self._repr

    def _repr_html_(self):
        return self._html

    def __getitem__(self, key):
        return self._data[key]

    def data_preprocessing(self, data):
        return data

    def save(self, path=None):
        if path is None:
            path = Path(tempfile.mktemp(suffix='.html'))

        path.write_text(self._html)

        return path

    def to_format(self, fmt):
        return tabulate(self._data, headers='keys', tablefmt=fmt)


class BuildReport(Table):
    """A Table that adds a columns for checking task elapsed time
    """

    def data_preprocessing(self, data):
        """Create a build report from several tasks
        """
        total = sum([row['Elapsed (s)'] or 0 for row in data])

        def compute_pct(elapsed, total):
            if not elapsed:
                return 0
            else:
                return 100 * elapsed / total

        for row in data:
            row['Percentage'] = compute_pct(row['Elapsed (s)'], total)

        return data
