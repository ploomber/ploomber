"""
Clients that communicate with databases
"""
try:
    import sqlalchemy
except ImportError:
    sqlalchemy = None

from ploomber.clients.client import Client
from ploomber.util import requires

import re
from pathlib import Path


def code_split(code, token=';'):
    only_whitespace = re.compile(r'^\s*$')

    for part in code.split(token):
        if not re.match(only_whitespace, part):
            yield part


def _get_url_obj(uri):
    if isinstance(uri, str):
        return sqlalchemy.engine.make_url(uri)
    elif isinstance(uri, sqlalchemy.engine.url.URL):
        return uri
    else:
        raise TypeError('SQLAlchemyClient must be initialized with a '
                        'string or a sqlalchemy.engine.url.URL object, got '
                        f'{type(uri).__name__}')


class DBAPIClient(Client):
    """A client for a PEP 249 compliant client library

    Parameters
    ----------
    connect_fn : callable
        The function to use to open the connection

    connect_kwargs : dict
        Keyword arguments to pass to connect_fn

    split_source : str, optional
        Some database drivers do not support multiple commands in a single
        execute statement. Use this optiion to split commands by a given
        character (e.g. ';') and send them one at a time. Defaults to
        None (no splitting)

    Examples
    --------
    >>> from ploomber.clients import DBAPIClient
    >>> import sqlite3
    >>> client = DBAPIClient(sqlite3.connect, dict(database='my.db'))
    """
    def __init__(self, connect_fn, connect_kwargs, split_source=None):
        super().__init__()
        self.connect_fn = connect_fn
        self.connect_kwargs = connect_kwargs
        self.split_source = split_source

        # there is no open connection by default
        self._connection = None

    @property
    def connection(self):
        """Return a connection, open one if there isn't any
        """
        # if there isn't an open connection, open one...
        if self._connection is None:
            self._connection = self.connect_fn(**self.connect_kwargs)

        return self._connection

    def cursor(self):
        return self.connection.cursor()

    def execute(self, code):
        """Execute code with the existing connection
        """
        cur = self.connection.cursor()

        if self.split_source:
            for command in code_split(code, token=self.split_source):
                cur.execute(command)
        else:
            cur.execute(code)

        self.connection.commit()
        cur.close()

    def close(self):
        """Close connection if there is an active one
        """
        if self._connection is not None:
            self._connection.close()

    def __getstate__(self):
        state = super().__getstate__()
        state['_connection'] = None
        return state


class SQLAlchemyClient(Client):
    """Client for connecting with any SQLAlchemy supported database

    Parameters
    ----------
    uri: str or sqlalchemy.engine.url.URL
        URI to pass to sqlalchemy.create_engine or URL object created using
        sqlalchemy.engine.url.URL.create

    split_source : str, optional
        Some database drivers do not support multiple commands in a single
        execute statement. Use this optiion to split commands by a given
        character (e.g. ';') and send them one at a time. Defaults to
        'default', which splits by ';' if using SQLite database,
        but does not perform any splitting with other databases. If None,
        it will never split, a string value is interpreted as the token
        to use for splitting statements regardless of the database type

    create_engine_kwargs : dict, optional
        Keyword arguments to pass to ``sqlalchemy.create_engine``

    Notes
    -----
    SQLite client does not support sending more than one command at a time,
    if using such backend code will be split and several calls to the db
    will be performed.

    Examples
    --------
    >>> from ploomber.clients import SQLAlchemyClient
    >>> client = SQLAlchemyClient('sqlite:///my_db.db')
    >>> import sqlalchemy
    >>> url = sqlalchemy.engine.url.URL.create(drivername='sqlite',
    ...                                        database='my_db.db')
    >>> client = SQLAlchemyClient(url)
    """
    split_source_mapping = {'sqlite': ';'}

    @requires(['sqlalchemy'], 'SQLAlchemyClient')
    def __init__(self, uri, split_source='default', create_engine_kwargs=None):
        super().__init__()
        self._uri = uri

        self._url_obj = _get_url_obj(uri)
        self._uri_safe = repr(self._url_obj)

        self._create_engine_kwargs = create_engine_kwargs or dict()
        self.flavor = self._url_obj.drivername
        self._engine = None
        self.split_source = split_source

        if self.flavor == "sqlite" and self._url_obj.database:
            parent_folder = Path(self._url_obj.database).parent
            parent_folder.mkdir(exist_ok=True, parents=True)

        self._connection = None

    @property
    def connection(self):
        """Return a connection from the pool
        """
        # we have to keep this reference here,
        # if we just return self.engine.raw_connection(),
        # any cursor from that connection will fail
        # doing: engine.raw_connection().cursor().execute('') fails!
        if self._connection is None:
            self._connection = self.engine.raw_connection()

        # if a task or product calls client.connection.close(), we have to
        # re-open the connection
        if not self._connection.is_valid:
            self._connection = self.engine.raw_connection()

        return self._connection

    def cursor(self):
        return self.connection.cursor()

    def execute(self, code):
        cur = self.connection.cursor()

        # if split_source is default, and there's a default token defined,
        # use it
        if (self.split_source == 'default'
                and self.flavor in self.split_source_mapping):
            token = self.split_source_mapping[self.flavor]
            for command in code_split(code, token=token):
                cur.execute(command)
        # otherwise interpret split_source as the token
        elif self.split_source:
            for command in code_split(code, token=self.split_source):
                cur.execute(command)
        # if no split_source, execute all at once
        else:
            cur.execute(code)

        self.connection.commit()
        cur.close()

    def close(self):
        """Closes all connections
        """
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

        if self._connection is not None:
            self._connection.close()

    @property
    def engine(self):
        """Returns a SQLAlchemy engine
        """
        if self._engine is None:
            self._engine = sqlalchemy.create_engine(
                self._uri, **self._create_engine_kwargs)

        return self._engine

    def __getstate__(self):
        state = super().__getstate__()
        state['_engine'] = None
        state['_connection'] = None
        return state

    def __str__(self):
        return self._uri_safe

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, self._uri_safe)
