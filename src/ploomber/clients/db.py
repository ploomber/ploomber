"""
Clients that communicate with databases
"""
try:
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None

from ploomber.clients.Client import Client
from ploomber.util import requires


class DBAPIClient(Client):
    """A client for a PEP 249 compliant client library

    Parameters
    ----------
    connect_fn: callable
        The function to use to open the connection
    **connect_kwargs: kwargs
        Keyword arguments to pass to connect_fn
    """
    def __init__(self, connect_fn, **connect_kwargs):
        super().__init__()
        self.connect_fn = connect_fn
        self.connect_kwargs = connect_kwargs

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

    def execute(self, code):
        """Execute code with the existing connection
        """
        cur = self.connection.cursor()
        cur.execute(code)
        self.connection.commit()
        cur.close()

    def close(self):
        """Close connection if there is one active
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
    uri: str
        URI to pass to sqlalchemy.create_engine

    Notes
    -----
    SQLite client does not support sending more than one command at a time,
    if using such backend code will be split and several calls to the db
    will be performed.
    """
    split_commands = ['sqlite']

    @requires(['sqlalchemy'], 'SQLAlchemyClient')
    def __init__(self, uri):
        super().__init__()
        self._uri = uri
        self.flavor = uri.split(':')[0]
        self._engine = None
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

    def execute(self, code):
        cur = self.connection.cursor()

        if self.flavor in self.split_commands:
            for command in code.split(';'):
                cur.execute(command)
        else:
            cur.execute(code)

        self.connection.commit()
        cur.close()

    def close(self):
        """Closes all connections
        """
        self._logger.info('Disposing engine %s', self._engine)
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    @property
    def engine(self):
        """Returns a SQLAlchemy engine
        """
        if self._engine is None:
            self._engine = create_engine(self._uri)

        return self._engine

    def __getstate__(self):
        state = super().__getstate__()
        state['_engine'] = None
        state['_connection'] = None
        return state


class DrillClient(Client):
    def __init__(self, params=dict(host='localhost', port=8047)):
        self.params = params
        self._set_logger()
        self._connection = None

    @property
    def connection(self):
        from pydrill.client import PyDrill

        if self._connection is None:
            self._connection = PyDrill(**self.params)

        return self._connection

    def execute(self, code):
        """Run code
        """
        return self.connection.query(code)

    def close(self):
        pass
