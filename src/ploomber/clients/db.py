"""
Clients that communicate with databases
"""
import re
from pathlib import Path

try:
    import sqlalchemy
except ImportError:
    sqlalchemy = None

from ploomber.clients.client import Client
from ploomber_core.dependencies import requires


def code_split(code, token=";"):
    only_whitespace = re.compile(r"^\s*$")

    for part in code.split(token):
        if not re.match(only_whitespace, part):
            yield part


def _get_url_obj(uri):
    if isinstance(uri, str):
        return sqlalchemy.engine.make_url(uri)
    elif isinstance(uri, sqlalchemy.engine.url.URL):
        return uri
    else:
        raise TypeError(
            "SQLAlchemyClient must be initialized with a "
            "string or a sqlalchemy.engine.url.URL object, got "
            f"{type(uri).__name__}"
        )


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

    Spec API:

    Given the following ``clients.py``:

    .. code-block:: python
        :class: text-editor
        :name: clients-py

        import sqlite3
        from ploomber.clients import DBAPIClient

        def get():
            return DBAPIClient(sqlite3.connect, dict(database='my.db'))


    Spec API (dag-level client):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        clients:
            # key is a task class such as SQLDump or SQLScript
            SQLDump: clients.get

        tasks:
            - source: query.sql
              product: output/data.csv


    Spec API (task-level client):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
            - source: query.sql
              product: output/data.csv
              client: clients.get


    Python API (dag-level client):

    >>> import sqlite3
    >>> import pandas as pd
    >>> from ploomber import DAG
    >>> from ploomber.products import File
    >>> from ploomber.tasks import SQLDump
    >>> from ploomber.clients import DBAPIClient
    >>> con_raw = sqlite3.connect(database='my.db')
    >>> df = pd.DataFrame({'a': range(100), 'b': range(100)})
    >>> _ = df.to_sql('numbers', con_raw, index=False)
    >>> con_raw.close()
    >>> dag = DAG()
    >>> client = DBAPIClient(sqlite3.connect, dict(database='my.db'))
    >>> dag.clients[SQLDump] = client # dag-level client
    >>> _ = SQLDump('SELECT * FROM numbers', File('data.parquet'),
    ...             dag=dag, name='dump',
    ...             client=client,
    ...             chunksize=None) # no need to pass client here
    >>> _ = dag.build()
    >>> df = pd.read_parquet('data.parquet')
    >>> df.head(3)
       a  b
    0  0  0
    1  1  1
    2  2  2


    Python API (task-level client):

    >>> import sqlite3
    >>> import pandas as pd
    >>> from ploomber import DAG
    >>> from ploomber.products import File
    >>> from ploomber.tasks import SQLDump
    >>> from ploomber.clients import DBAPIClient
    >>> con_raw = sqlite3.connect(database='some.db')
    >>> df = pd.DataFrame({'a': range(100), 'b': range(100)})
    >>> _ = df.to_sql('numbers', con_raw, index=False)
    >>> con_raw.close()
    >>> dag = DAG()
    >>> client = DBAPIClient(sqlite3.connect, dict(database='some.db'))
    >>> _ = SQLDump('SELECT * FROM numbers', File('data.parquet'),
    ...             dag=dag, name='dump',
    ...             client=client,  # pass client to task
    ...             chunksize=None)
    >>> _ = dag.build()
    >>> df = pd.read_parquet('data.parquet')
    >>> df.head(3)
       a  b
    0  0  0
    1  1  1
    2  2  2


    See Also
    --------
    ploomber.clients.SQLAlchemyClient :
        A client to connect to a database using sqlalchemy as backend

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
        """Return a connection, open one if there isn't any"""
        # if there isn't an open connection, open one...
        if self._connection is None:
            self._connection = self.connect_fn(**self.connect_kwargs)

        return self._connection

    def cursor(self):
        return self.connection.cursor()

    def execute(self, code):
        """Execute code with the existing connection"""
        cur = self.connection.cursor()

        if self.split_source:
            for command in code_split(code, token=self.split_source):
                cur.execute(command)
        else:
            cur.execute(code)

        self.connection.commit()
        cur.close()

    def close(self):
        """Close connection if there is an active one"""
        if self._connection is not None:
            self._connection.close()

    def __getstate__(self):
        state = super().__getstate__()
        state["_connection"] = None
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
        execute statement. Use this option to split commands by a given
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

    Spec API:

    Given the following ``clients.py``:

    .. code-block:: python
        :class: text-editor
        :name: clients-py

        import sqlalchemy
        from ploomber.clients import SQLAlchemyClient

        def get():
            url = sqlalchemy.engine.url.URL.create(drivername='sqlite',
                                                   database='my_db.db')
            return SQLAlchemyClient(url)


    Spec API (dag-level client):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        clients:
            # key is a task class such as SQLDump or SQLScript
            SQLDump: clients.get

        tasks:
            - source: query.sql
              product: output/data.csv


    Spec API (task-level client):

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        tasks:
            - source: query.sql
              product: output/data.csv
              client: clients.get

    Python API (dag-level client):

    >>> import sqlite3
    >>> import sqlalchemy
    >>> import pandas as pd
    >>> from ploomber import DAG
    >>> from ploomber.products import File
    >>> from ploomber.tasks import SQLDump
    >>> from ploomber.clients import SQLAlchemyClient
    >>> con_raw = sqlite3.connect(database='my.db')
    >>> df = pd.DataFrame({'a': range(100), 'b': range(100)})
    >>> _ = df.to_sql('numbers', con_raw, index=False)
    >>> con_raw.close()
    >>> dag = DAG()
    >>> url = sqlalchemy.engine.url.URL.create(drivername='sqlite',
    ...                                        database='my.db')
    >>> client = SQLAlchemyClient(url)
    >>> dag.clients[SQLDump] = client # dag-level client
    >>> _ = SQLDump('SELECT * FROM numbers', File('data.parquet'),
    ...             dag=dag, name='dump',
    ...             chunksize=None) # no need to pass client here
    >>> _ = dag.build()
    >>> df = pd.read_parquet('data.parquet')
    >>> df.head(3)
       a  b
    0  0  0
    1  1  1
    2  2  2

    Python API (task-level client):

    >>> import sqlite3
    >>> import sqlalchemy
    >>> import pandas as pd
    >>> from ploomber import DAG
    >>> from ploomber.products import File
    >>> from ploomber.tasks import SQLDump
    >>> from ploomber.clients import SQLAlchemyClient
    >>> con_raw = sqlite3.connect(database='some.db')
    >>> df = pd.DataFrame({'a': range(100), 'b': range(100)})
    >>> _ = df.to_sql('numbers', con_raw, index=False)
    >>> con_raw.close()
    >>> dag = DAG()
    >>> url = sqlalchemy.engine.url.URL.create(drivername='sqlite',
    ...                                        database='some.db')
    >>> client = SQLAlchemyClient(url)
    >>> _ = SQLDump('SELECT * FROM numbers', File('data.parquet'),
    ...             dag=dag, name='dump',
    ...             client=client, # pass client to task
    ...             chunksize=None)
    >>> _ = dag.build()
    >>> df = pd.read_parquet('data.parquet')
    >>> df.head(3)
       a  b
    0  0  0
    1  1  1
    2  2  2

    See Also
    --------
    ploomber.clients.DBAPIClient :
        A client to connect to a database
    """

    split_source_mapping = {"sqlite": ";"}

    @requires(["sqlalchemy"], "SQLAlchemyClient")
    def __init__(self, uri, split_source="default", create_engine_kwargs=None):
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
        """Return a connection from the pool"""
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
        if self.split_source == "default" and self.flavor in self.split_source_mapping:
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
        """Closes all connections"""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

        if self._connection is not None:
            self._connection.close()

    @property
    def engine(self):
        """Returns a SQLAlchemy engine"""
        if self._engine is None:
            self._engine = sqlalchemy.create_engine(
                self._uri, **self._create_engine_kwargs
            )

        return self._engine

    def __getstate__(self):
        state = super().__getstate__()
        state["_engine"] = None
        state["_connection"] = None
        return state

    def __str__(self):
        return self._uri_safe

    def __repr__(self):
        return "{}({})".format(type(self).__name__, self._uri_safe)
