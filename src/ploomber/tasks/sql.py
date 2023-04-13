from pathlib import Path
from io import StringIO

from jinja2 import Template

from ploomber.tasks.abc import Task
from ploomber.tasks.mixins import ClientMixin
from ploomber.sources import SQLScriptSource, SQLQuerySource, FileSource
from ploomber.products import (
    File,
    PostgresRelation,
    SQLiteRelation,
    GenericSQLRelation,
    GenericProduct,
    SQLRelation,
)
from ploomber import io
from ploomber_core.dependencies import requires
from ploomber.placeholders.placeholder import _add_globals
from ploomber.exceptions import SQLTaskBuildError


class SQLScript(ClientMixin, Task):
    """Execute a script in a SQL database to create a relation or view

    Parameters
    ----------
    source: str or pathlib.Path
        SQL script source, if str, the content is interpreted as the actual
        script, if pathlib.Path, the content of the file is loaded
    product: ploomber.products.product
        Product generated upon successful execution
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    client: ploomber.clients.{SQLAlchemyClient, DBAPIClient}, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]
    params: dict, optional
        Parameters to pass to the script, by default, the callable will
        be executed with a "product" (which will contain the product object).
        It will also include a "upstream" parameter if the task has upstream
        dependencies along with any parameters declared here. The source
        code is converted to a jinja2.Template for passing parameters,
        refer to jinja2 documentation for details

    Examples
    --------

    Spec API:

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        clients:
          SQLScript: clients.get
          SQLiteRelation: clients.get

        tasks:
          - source: script.sql
            product: [subset, table]

    Python API (SQLite):

    >>> import sqlite3
    >>> import pandas as pd
    >>> from ploomber import DAG
    >>> from ploomber.products import SQLiteRelation
    >>> from ploomber.tasks import SQLScript
    >>> from ploomber.clients import DBAPIClient
    >>> con_raw = sqlite3.connect(database='my_db.db')
    >>> df = pd.DataFrame({'a': range(100), 'b': range(100)})
    >>> _ = df.to_sql('numbers', con_raw, index=False)
    >>> dag = DAG()
    >>> client = DBAPIClient(sqlite3.connect, dict(database='my_db.db'),
    ...                      split_source=';')
    >>> dag.clients[SQLScript] = client
    >>> dag.clients[SQLiteRelation] = client
    >>> script = ('DROP TABLE IF EXISTS {{product}};'
    ...           'CREATE TABLE {{product}} AS '
    ...           'SELECT * FROM numbers LIMIT 3')
    >>> _ = SQLScript(script, SQLiteRelation(('subset', 'table')),
    ...               dag=dag, name='create-subset')
    >>> _ = dag.build()
    >>> df = pd.read_sql('SELECT * FROM subset', con_raw)
    >>> con_raw.close()
    >>> df.head(3)
       a  b
    0  0  0
    1  1  1
    2  2  2

    See Also
    --------
    ploomber.clients.SQLDump :
        A task to execute a ``SELECT`` statement and dump the output into
        a file
    """

    PRODUCT_CLASSES_ALLOWED = (
        PostgresRelation,
        SQLiteRelation,
        GenericSQLRelation,
        SQLRelation,
    )

    def __init__(self, source, product, dag, name=None, client=None, params=None):
        params = params or {}
        # TODO: access self.client so it uses the dag-level if available
        try:
            split_source = client.split_source
        except AttributeError:
            split_source = None

        kwargs = dict(hot_reload=dag._params.hot_reload, split_source=split_source)

        self._source = type(self)._init_source(source, kwargs)
        super().__init__(product, dag, name, params)
        self._client = client
        self.dag = dag

    def run(self):
        source_code = str(self.source)

        try:
            return self.client.execute(source_code)
        except Exception as e:
            raise SQLTaskBuildError(type(self), source_code, e) from e

    def load(self, limit=10):
        """Load this task's product in a pandas.DataFrame

        Parameters
        ----------
        limit : int, default=10
            How many records to load, defaults to 10
        """
        import pandas as pd

        return pd.read_sql(
            f"SELECT * FROM {self.product} LIMIT {int(limit)}", self.client
        )

    @staticmethod
    def _init_source(source, kwargs):
        return SQLScriptSource(source, **kwargs)


class SQLDump(io.FileLoaderMixin, ClientMixin, Task):
    """Dumps data from a SQL SELECT statement to a file(s)

    Parameters
    ----------
    source: str or pathlib.Path
        SQL script source, if str, the content is interpreted as the actual
        script, if pathlib.Path, the content of the file is loaded
    product: ploomber.products.product
        Product generated upon successful execution
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    client: ploomber.clients.{SQLAlchemyClient, DBAPIClient}, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]
    params: dict, optional
        Parameters to pass to the script, by default, the callable will
        be executed with a "product" (which will contain the product object).
        It will also include a "upstream" parameter if the task has upstream
        dependencies along with any parameters declared here. The source
        code is converted to a jinja2.Template for passing parameters,
        refer to jinja2 documentation for details
    chunksize: int, optional
        Number of rows per file, otherwise download the entire dataset in
        a single one. If not None, the product becomes a directory
    io_handler: ploomber.io.CSVIO or ploomber.io.ParquetIO, optional
        io handler to use (which controls the output format), currently
        only csv and parquet are supported. If None, it tries to infer the
        handler from the product's extension if that doesn't work, it uses
        io.CSVIO

    Examples
    --------

    Spec API:

    .. code-block:: yaml
        :class: text-editor
        :name: pipeline-yaml

        clients:
          # define a get function in clients.py that returns the client
          SQLDump: clients.get

        tasks:
          # script with a SELECT statement
          - source: script.sql
            product: data.parquet

    `Full spec API example. <https://github.com/ploomber/projects/tree/master/cookbook/sql-dump>`_

    Python API:

    >>> import sqlite3
    >>> import pandas as pd
    >>> from ploomber import DAG
    >>> from ploomber.products import File
    >>> from ploomber.tasks import SQLDump
    >>> from ploomber.clients import DBAPIClient
    >>> con_raw = sqlite3.connect(database='my_db.db')
    >>> df = pd.DataFrame({'a': range(100), 'b': range(100)})
    >>> _ = df.to_sql('numbers', con_raw, index=False)
    >>> con_raw.close()
    >>> dag = DAG()
    >>> client = DBAPIClient(sqlite3.connect, dict(database='my_db.db'))
    >>> _ = SQLDump('SELECT * FROM numbers', File('data.parquet'),
    ...             dag=dag, name='dump', client=client, chunksize=None)
    >>> _ = dag.build()
    >>> df = pd.read_parquet('data.parquet')
    >>> df.head(3)
       a  b
    0  0  0
    1  1  1
    2  2  2

    Notes
    -----
    The chunksize parameter is also set in cursor.arraysize object, this
    parameter can greatly speed up the dump for some databases when the
    driver uses cursors.arraysize as the number of rows to fetch on a single
    network trip, but this is driver-dependent, not all drivers implement
    this (cx_Oracle does it)

    See Also
    --------
    ploomber.clients.SQLScript :
        A task to execute a SQL script and create a table/view as product
    """  # noqa

    PRODUCT_CLASSES_ALLOWED = (File, GenericProduct)

    def __init__(
        self,
        source,
        product,
        dag,
        name=None,
        client=None,
        params=None,
        chunksize=10000,
        io_handler=None,
    ):
        params = params or {}

        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(source, kwargs)
        super().__init__(product, dag, name, params)

        self._client = client
        self.chunksize = chunksize

        if io_handler is None:
            if self.product._identifier._raw.endswith(".parquet"):
                self.io_handler = io.ParquetIO
            else:
                self.io_handler = io.CSVIO
        else:
            self.io_handler = io_handler

    @staticmethod
    def _init_source(source, kwargs):
        return SQLQuerySource(source, **kwargs)

    def run(self):
        # render runtime parameters
        template = Template(
            str(self.source), variable_start_string="[[", variable_end_string="]]"
        )
        _add_globals(template.environment)
        source_code = template.render(upstream=self.params.get("upstream"))

        path = Path(str(self.params["product"]))
        handler = self.io_handler(path, chunked=bool(self.chunksize))

        self._logger.debug("Code: %s", source_code)

        cursor = self.client.connection.cursor()

        try:
            cursor.execute(source_code)
        except Exception as e:
            raise SQLTaskBuildError(type(self), source_code, e) from e

        if self.chunksize:
            i = 1
            headers = None
            cursor.arraysize = self.chunksize

            while True:
                self._logger.info("Fetching chunk {}...".format(i))
                data = cursor.fetchmany()
                self._logger.info("Fetched chunk {}".format(i))

                if i == 1:
                    headers = [c[0] for c in cursor.description]

                if not data:
                    break

                handler.write(data, headers)

                i = i + 1
        else:
            data = cursor.fetchall()
            headers = [c[0] for c in cursor.description]
            handler.write(data, headers)

        cursor.close()


# FIXME: this can be a lot faster for clients that transfer chunksize
# rows over the network
class SQLTransfer(ClientMixin, Task):
    """
    Transfers data from a SQL database to another (Note: this relies on
    pandas, only use it for small to medium size datasets)

    Parameters
    ----------
    source: str or pathlib.Path
        SQL script source, if str, the content is interpreted as the actual
        script, if pathlib.Path, the content of the file is loaded
    product: ploomber.products.product
        Product generated upon successful execution. For SQLTransfer, usually
        product.client != task.client. task.client represents the data source
        while product.client represents the data destination
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    client: ploomber.clients.SQLAlchemyClient, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]
    params: dict, optional
        Parameters to pass to the script, by default, the callable will
        be executed with a "product" (which will contain the product object).
        It will also include a "upstream" parameter if the task has upstream
        dependencies along with any parameters declared here. The source
        code is converted to a jinja2.Template for passing parameters,
        refer to jinja2 documentation for details
    chunksize: int, optional
        Number of rows to transfer on every chunk

    Notes
    ----
    This task is *not* intended to move large datasets, but a
    convenience way of transfering small to medium size datasets. It relies
    on pandas to read and write, which introduces a considerable overhead.
    """

    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation, GenericSQLRelation)

    @requires(["pandas"], "SQLTransfer")
    def __init__(
        self, source, product, dag, name=None, client=None, params=None, chunksize=10000
    ):
        params = params or {}
        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(source, kwargs)
        super().__init__(product, dag, name, params)
        self._client = client
        self.chunksize = chunksize

    @staticmethod
    def _init_source(source, kwargs):
        # TODO: this shoule be a FileSource
        return SQLQuerySource(source, **kwargs)

    def run(self):
        import pandas as pd

        source_code = str(self.source)
        product = self.params["product"]

        # read from source_code, use connection from the Task
        self._logger.info("Fetching data...")
        dfs = pd.read_sql_query(
            source_code, self.client.engine, chunksize=self.chunksize
        )
        self._logger.info("Done fetching data...")

        for i, df in enumerate(dfs):
            self._logger.info("Storing chunk {i}...".format(i=i))
            df.to_sql(
                name=product.name,
                con=product.client.engine,
                schema=product.schema,
                if_exists="replace" if i == 0 else "append",
                index=False,
            )


class SQLUpload(ClientMixin, Task):
    """
    Upload data to a SQL database from a parquet or a csv file. Note: this
    task relies uses pandas.to_sql which introduces some overhead. Only use it
    for small to medium size datasets. Each database usually come with a tool
    to upload data efficiently. If you are using PostgreSQL, check out the
    PostgresCopyFrom task.

    Parameters
    ----------
    source : str or pathlib.Path
        Path to parquet or a csv file to upload

    product : ploomber.products.product
        Product generated upon successful execution. The client for the product
        must be in the target database, where as task.client should be a client
        in the source database.

    dag : ploomber.DAG
        A DAG to add this task to

    name : str
        A str to indentify this task. Should not already exist in the dag

    client: ploomber.clients.SQLAlchemyClient, optional
        The client used to connect to the database and where the data will be
        uploaded. Only required
        if no dag-level client has been declared using dag.clients[class]

    params : dict, optional
        Parameters to pass to the script, by default, the callable will
        be executed with a "product" (which will contain the product object).
        It will also include a "upstream" parameter if the task has upstream
        dependencies along with any parameters declared here. The source
        code is converted to a jinja2.Template for passing parameters,
        refer to jinja2 documentation for details

    chunksize : int, optional
        Number of rows to transfer on every chunk

    io_handler : callable, optional
        A Python callable to read the source file,
        if None, it will tried to be inferred from the source file extension

    to_sql_kwargs : dict, optional
        Keyword arguments passed to the pandas.DataFrame.to_sql function,
        one useful parameter is "if_exists", which determines if the
        task should fail ("fail"), the relation should be replaced
        ("replace") or rows appended ("append").

    Notes
    -----
    This task is *not* intended to move large datasets, but a
    convenience way of transfering small to medium size datasets. It relies
    on pandas to read and write, which introduces a considerable overhead.
    """

    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation, GenericSQLRelation)

    @requires(["pandas"], "SQLUpload")
    def __init__(
        self,
        source,
        product,
        dag,
        name=None,
        client=None,
        params=None,
        chunksize=None,
        io_handler=None,
        to_sql_kwargs=None,
    ):
        params = params or {}
        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(source, kwargs)
        super().__init__(product, dag, name, params)
        self._client = client
        self.chunksize = chunksize
        self.io_handler = io_handler
        self.to_sql_kwargs = to_sql_kwargs or {}

    @staticmethod
    def _init_source(source, kwargs):
        return FileSource(str(source), **kwargs)

    def run(self):
        import pandas as pd

        product = self.params["product"]
        path = str(self.source)

        mapping = {
            ".csv": pd.read_csv,
            ".parquet": pd.read_parquet,
        }

        if self.io_handler is None:
            extension = Path(path).suffix
            read_fn = mapping.get(extension)

            if not read_fn:
                raise ValueError(
                    "Could not infer reading function for "
                    "file with extension: {}".format(extension),
                    "pass the function directly in the " "io_handler argument",
                )
        else:
            read_fn = self.io_handler

        self._logger.info("Reading data...")
        df = read_fn(path)
        self._logger.info("Done reading data...")

        df.to_sql(
            name=product.name,
            con=self.client.engine,
            schema=product.schema,
            **self.to_sql_kwargs,
        )


# TODO: provide more flexibility to configure the COPY statement
class PostgresCopyFrom(ClientMixin, Task):
    """Efficiently copy data to a postgres database using COPY FROM (faster
    alternative to SQLUpload for postgres). If using SQLAlchemy client
    for postgres is psycopg2. Replaces the table if exists.

    Parameters
    ----------
    source: str or pathlib.Path
        Path to parquet file to upload

    client: ploomber.clients.SQLAlchemyClient, optional
        The client used to connect to the database and where the data will be
        uploaded. Only required
        if no dag-level client has been declared using dag.clients[class]


    Notes
    -----
    Although this task does not depend on pandas for data i/o, it still
    needs it to dynamically create the table, after the table is created
    the COPY statement is used to upload the data
    """

    PRODUCT_CLASSES_ALLOWED = (PostgresRelation,)

    @requires(["pandas", "psycopg2"], "PostgresCopyFrom")
    def __init__(
        self, source, product, dag, name=None, client=None, params=None, columns=None
    ):
        params = params or {}
        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = type(self)._init_source(source, kwargs)
        super().__init__(product, dag, name, params)
        self._client = client
        self.columns = columns

    @staticmethod
    def _init_source(source, kwargs):
        return FileSource(str(source), **kwargs)

    def run(self):
        import pandas as pd

        product = self.params["product"]
        df = pd.read_parquet(str(self.source))

        # create the table
        self._logger.info("Creating table...")
        df.head(0).to_sql(
            name=product.name,
            con=self.client.engine,
            schema=product.schema,
            if_exists="replace",
            index=False,
        )
        self._logger.info("Done creating table.")

        # create file-like object
        f = StringIO()
        df.to_csv(f, sep="\t", na_rep="\\N", header=False, index=False)
        f.seek(0)

        # upload using copy
        cur = self.client.connection.cursor()

        self._logger.info("Copying data...")
        cur.copy_expert(f"COPY {product} FROM STDIN", f)

        f.close()
        cur.close()
