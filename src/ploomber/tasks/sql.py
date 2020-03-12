from pathlib import Path
from io import StringIO

from ploomber.tasks.Task import Task
from ploomber.sources import (SQLScriptSource,
                              SQLQuerySource,
                              FileSource)
from ploomber.products import File, PostgresRelation, SQLiteRelation
from ploomber import io
from ploomber.util import requires

import pandas as pd


class SQLScript(Task):
    """Execute a script in a SQL database

    Parameters
    ----------
    source: str or pathlib.Path
        SQL script source, if str, the content is interpreted as the actual
        script, if pathlib.Path, the content of the file is loaded
    product: ploomber.products.Product
        Product generated upon successful execution
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    client: ploomber.clients.DBAPIClient or SQLAlchemyClient, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]
    params: dict, optional
        Parameters to pass to the script, by default, the callable will
        be executed with a "product" (which will contain the product object).
        It will also include a "upstream" parameter if the task has upstream
        dependencies along with any parameters declared here. The source
        code is converted to a jinja2.Template for passing parameters,
        refer to jinja2 documentation for details
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    def __init__(self, source, product, dag, name=None, client=None,
                 params=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def run(self):
        return self.client.execute(self.source_code)

    def _init_source(self, source):
        return SQLScriptSource(source)


class SQLDump(Task):
    """Dumps data from a SQL SELECT statement to a file(s)

    Parameters
    ----------
    source: str or pathlib.Path
        SQL script source, if str, the content is interpreted as the actual
        script, if pathlib.Path, the content of the file is loaded
    product: ploomber.products.Product
        Product generated upon successful execution
    dag: ploomber.DAG
        A DAG to add this task to
    name: str
        A str to indentify this task. Should not already exist in the dag
    client: ploomber.clients.DBAPIClient or SQLAlchemyClient, optional
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
        a single one. If set, the product will be a folder
    io_handler: ploomber.io.CSVIO or ploomber.io.ParquetIO, optional
        io handler to use (which controls the output format), currently
        only csv and parquet are supported. Defaults to ploomber.io.CSVIO

    Notes
    -----
    The chunksize parameter is also set in cursor.arraysize object, this
    parameter can greatly speed up the dump for some databases when the
    driver uses cursors.arraysize as the number of rows to fetch on a single
    network trip, but this is driver-dependent, not all drivers implement
    this (cx_Oracle does it)
    """
    PRODUCT_CLASSES_ALLOWED = (File, )

    def __init__(self, source, product, dag, name=None, client=None,
                 params=None,
                 chunksize=10000, io_handler=io.CSVIO):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))
        self.chunksize = chunksize
        self.io_handler = io_handler

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def _init_source(self, source):
        return SQLQuerySource(source)

    def run(self):
        source_code = str(self.source)
        path = Path(str(self.params['product']))
        handler = self.io_handler(path, chunked=bool(self.chunksize))

        self._logger.debug('Code: %s', source_code)

        cursor = self.client.connection.cursor()
        cursor.execute(source_code)

        if self.chunksize:
            i = 1
            headers = None
            cursor.arraysize = self.chunksize

            while True:
                self._logger.info('Fetching chunk {}...'.format(i))
                data = cursor.fetchmany()
                self._logger.info('Fetched chunk {}'.format(i))

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


class SQLTransfer(Task):
    """
    Transfers data from a SQL database to another (Note: this relies on
    pandas, only use it for small to medium size datasets)

    Parameters
    ----------
    source: str or pathlib.Path
        SQL script source, if str, the content is interpreted as the actual
        script, if pathlib.Path, the content of the file is loaded
    product: ploomber.products.Product
        Product generated upon successful execution. For SQLTransfer, usually
        product.client != task.client. task.client represents the data source
        while product.client represents the data destination.
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
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    @requires(['pandas'], 'SQLTransfer')
    def __init__(self, source, product, dag, name=None, client=None,
                 params=None, chunksize=10000):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.chunksize = chunksize

    def _init_source(self, source):
        # TODO: this shoule be a FileSource
        return SQLQuerySource(source)

    def run(self):
        source_code = str(self.source)
        product = self.params['product']

        # read from source_code, use connection from the Task
        self._logger.info('Fetching data...')
        dfs = pd.read_sql_query(source_code, self.client.engine,
                                chunksize=self.chunksize)
        self._logger.info('Done fetching data...')

        for i, df in enumerate(dfs):
            self._logger.info('Storing chunk {i}...'.format(i=i))
            df.to_sql(name=product.name,
                      con=product.client.engine,
                      schema=product.schema,
                      if_exists='replace' if i == 0 else 'append',
                      index=False)


# FIXME: support other data formats apart from parquet
class SQLUpload(Task):
    """
    Upload data to a SQL database from a parquet file (Note: this relies on
    pandas, only use it for small to medium size datasets)

    Parameters
    ----------
    source: str or pathlib.Path
        Path to parquet file to upload
    product: ploomber.products.Product
        Product generated upon successful execution. For SQLTransfer, usually
        product.client != task.client. task.client represents the data source
        while product.client represents the data destination.
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
    io_handler : callable, optional
        A Python callable to read the source file,
        if None, it will tried to be infered from the source file extension

    Notes
    -----
    This task is *not* intended to move large datasets, but a
    convenience way of transfering small to medium size datasets. It relies
    on pandas to read and write, which introduces a considerable overhead.
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    @requires(['pandas'], 'SQLUpload')
    def __init__(self, source, product, dag, name=None, client=None,
                 params=None, chunksize=None, io_handler=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.chunksize = chunksize
        self.io_handler = io_handler

    def _init_source(self, source):
        return FileSource(str(source))

    def run(self):
        product = self.params['product']
        path = str(self.source)

        mapping = {
            '.csv': pd.read_csv,
            '.parquet': pd.read_parquet,
        }

        if self.io_handler is None:
            extension = Path(path).suffix
            read_fn = mapping.get(extension)

            if not read_fn:
                raise ValueError('Could not infer reading function for '
                                 'file with extension: {}'.format(extension),
                                 'pass the function directly in the '
                                 'io_handler argument')

        self._logger.info('Reading data...')
        df = read_fn(path)
        self._logger.info('Done reading data...')

        df.to_sql(name=product.name,
                  con=product.client.engine,
                  schema=product.schema,
                  if_exists='replace',
                  index=False)


# TODO: provide more flexibility to configure the COPY statement
class PostgresCopyFrom(Task):
    """Efficiently copy data to a postgres database using COPY FROM (faster
    alternative to SQLUpload for postgres). If using SQLAlchemy client
    for postgres is psycopg2. Replaces the table if exists.

    Parameters
    ----------
    source: str or pathlib.Path
        Path to parquet file to upload


    Notes
    -----
    Although this task does not depend on pandas for data i/o, it still
    needs it to dynamically create the table, after the table is created
    the COPY statement is used to upload the data
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation,)

    @requires(['pandas', 'psycopg2'], 'PostgresCopyFrom')
    def __init__(self, source, product, dag, name=None, client=None,
                 params=None, sep='\t', null='\\N', columns=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.sep = sep
        self.null = null
        self.columns = columns

    def _init_source(self, source):
        return FileSource(str(source))

    def run(self):
        product = self.params['product']
        df = pd.read_parquet(str(self.source))

        # create the table
        self._logger.info('Creating table...')
        df.head(0).to_sql(name=product.name,
                          con=product.client.engine,
                          schema=product.schema,
                          if_exists='replace',
                          index=False)
        self._logger.info('Done creating table.')

        # if product.kind != 'table':
        #     raise ValueError('COPY is only supportted in tables')

        # create file-like object
        f = StringIO()
        df.to_csv(f, sep='\t', na_rep='\\N', header=False, index=False)
        f.seek(0)

        # upload using copy
        cur = self.client.connection.cursor()

        self._logger.info('Copying data...')
        cur.copy_from(f,
                      table=str(product),
                      sep='\t',
                      null='\\N')

        f.close()
        cur.close()
