from pathlib import Path
from io import StringIO

from ploomber.exceptions import SourceInitializationError
from ploomber.tasks.Task import Task
from ploomber.sources import (SQLScriptSource,
                                      SQLQuerySource,
                                      GenericSource)
from ploomber.products import File, PostgresRelation, SQLiteRelation
from ploomber import io

import pandas as pd


class SQLScript(Task):
    """
    A tasks represented by a SQL script run agains a database this Task
    does not make any assumptions about the underlying SQL engine, it should
    work witn all DBs supported by SQLAlchemy
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    def __init__(self, source, product, dag, name, client=None,
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
    """
    Dumps data from a SQL SELECT statement to parquet files (one per chunk)

    Parameters
    ----------
    source: str
        The SQL query to run in the database
    product: File
        The directory location for the output parquet files
    dag: DAG
        The DAG for this task
    name: str
        Name for this task
    params: dict, optional
        Extra parameters for the task
    chunksize: int, optional
        Size of each chunk, one parquet file will be generated per chunk. If
        None, only one file is created


    Notes
    -----
    The chunksize parameter is set in cursor.arraysize object, this parameter
    can greatly speed up the dump for some databases when the driver uses
    cursors.arraysize as the number of rows to fetch on a single call
    """
    PRODUCT_CLASSES_ALLOWED = (File, )

    def __init__(self, source, product, dag, name, client=None,
                 params=None,
                 chunksize=10000, io_handler=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))
        self.chunksize = chunksize
        self.io_handler = io_handler or io.CSVIO

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
    """Transfers data from a SQL statement to a SQL relation
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    def __init__(self, source, product, dag, name, client=None,
                 params=None, chunksize=10000):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

        self.chunksize = chunksize

    def _init_source(self, source):
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


class SQLUpload(Task):
    """Upload data to a database from a parquet file

    Parameters
    ----------
    source: str or pathlib.Path
        Path to parquet file to upload
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation, SQLiteRelation)

    def __init__(self, source, product, dag, name, client=None,
                 params=None):
        params = params or {}
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a connection'
                             .format(type(self).__name__))

    def _init_source(self, source):
        source = GenericSource(str(source))

        if source.needs_render:
            raise SourceInitializationError('{} does not support templates as '
                                            'source, pass a path to a file',
                                            self.__class__)

        return source

    def run(self):
        product = self.params['product']

        self._logger.info('Reading data...')
        df = pd.read_parquet(str(self.source))
        self._logger.info('Done reading data...')

        df.to_sql(name=product.name,
                  con=product.client.engine,
                  schema=product.schema,
                  if_exists='replace',
                  index=False)


class PostgresCopy(Task):
    """Efficiently copy data to a postgres database using COPY (better
    alternative to SQLUpload for postgres)

    Parameters
    ----------
    source: str or pathlib.Path
        Path to parquet file to upload
    """
    PRODUCT_CLASSES_ALLOWED = (PostgresRelation,)

    def __init__(self, source, product, dag, name, client=None,
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
        source = GenericSource(str(source))

        if source.needs_render:
            raise SourceInitializationError('{} does not support templates as '
                                            'source, pass a path to a file',
                                            self.__class__)

        return source

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
