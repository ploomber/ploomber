import sqlite3
import json

from ploomber.products import Product
from ploomber.products.serializers import Base64Serializer
from ploomber.templates.Placeholder import SQLRelationPlaceholder


class SQLiteRelation(Product):
    """A SQLite relation

    Parameters
    ----------
    identifier: tuple of length 3
        A tuple with (schema, name, kind) where kind must be either 'table'
        or 'view'
    client: ploomber.clients.DBAPIClient or SQLAlchemyClient, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]
    """

    def __init__(self, identifier, client=None):
        super().__init__(identifier)
        self._client = client

    def _init_identifier(self, identifier):
        return SQLRelationPlaceholder(identifier)

    @property
    def client(self):
        # FIXME: this nested reference looks ugly
        if self._client is None:
            default = self.task.dag.clients.get(type(self))

            if default is None:
                raise ValueError('{} must be initialized with a client'
                                 .format(type(self).__name__))
            else:
                self._client = default

        return self._client

    def _create_metadata_relation(self):

        create_metadata = """
        CREATE TABLE IF NOT EXISTS _metadata (
            name TEXT PRIMARY KEY,
            metadata BLOB
        )
        """
        self.client.execute(create_metadata)

    def fetch_metadata(self):
        self._create_metadata_relation()

        query = """
        SELECT metadata FROM _metadata
        WHERE name = '{name}'
        """.format(name=self._identifier.name)

        cur = self.client.connection.cursor()
        cur.execute(query)
        records = cur.fetchone()
        cur.close()

        if records:
            metadata_bin = records[0]
            return json.loads(metadata_bin.decode("utf-8"))
        else:
            return None

    def save_metadata(self, metadata):
        self._create_metadata_relation()

        metadata_bin = json.dumps(metadata).encode('utf-8')

        query = """
            REPLACE INTO _metadata(metadata, name)
            VALUES(?, ?)
        """
        cur = self.client.connection.cursor()
        cur.execute(query, (sqlite3.Binary(metadata_bin),
                            self._identifier.name))
        self.client.connection.commit()
        cur.close()

    def exists(self):
        query = """
        SELECT name
        FROM sqlite_master
        WHERE type = '{kind}'
        AND name = '{name}'
        """.format(kind=self._identifier.kind,
                   name=self._identifier.name)

        cur = self.client.connection.cursor()
        cur.execute(query)
        exists = cur.fetchone() is not None
        cur.close()
        return exists

    def delete(self):
        """Deletes the product
        """
        query = ("DROP {kind} IF EXISTS {relation}"
                 .format(kind=self._identifier.kind,
                         relation=str(self)))
        self.logger.debug('Running "{query}" on the databse...'
                          .format(query=query))
        self.client.execute(query)

    @property
    def name(self):
        return self._identifier.name

    @property
    def schema(self):
        return self._identifier.schema

    @property
    def kind(self):
        return self._identifier.kind


class PostgresRelation(Product):
    """A PostgreSQL relation

    Parameters
    ----------
    identifier: tuple of length 3
        A tuple with (schema, name, kind) where kind must be either 'table'
        or 'view'
    client: ploomber.clients.DBAPIClient or SQLAlchemyClient, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]
    """
    # FIXME: identifier has schema as optional but that introduces ambiguity
    # when fetching metadata and checking if the table exists so maybe it
    # should be required

    def __init__(self, identifier, client=None):
        self._client = client
        super().__init__(identifier)

    def _init_identifier(self, identifier):
        return SQLRelationPlaceholder(identifier)

    @property
    def client(self):
        if self._client is None:
            default = self.task.dag.clients.get(type(self))

            if default is None:
                raise ValueError('{} must be initialized with a client'
                                 .format(type(self).__name__))
            else:
                self._client = default

        return self._client

    def fetch_metadata(self):
        cur = self.client.connection.cursor()

        if self._identifier.schema:
            schema = self._identifier.schema
        else:
            # if schema is empty, we have to find out the default one
            query = """
            SELECT replace(setting, '"$user", ', '')
            FROM pg_settings WHERE name = 'search_path';
            """
            cur.execute(query)
            schema = cur.fetchone()[0]

        # https://stackoverflow.com/a/11494353/709975
        query = """
        SELECT description
        FROM pg_description
        JOIN pg_class ON pg_description.objoid = pg_class.oid
        JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
        WHERE nspname = %(schema)s
        AND relname = %(name)s
        """
        cur.execute(query, dict(schema=schema,
                                name=self._identifier.name))
        metadata = cur.fetchone()

        cur.close()

        # no metadata saved
        if metadata is None:
            return None
        else:
            return Base64Serializer.deserialize(metadata[0])

        # TODO: also check if metadata  does not give any parsing errors,
        # if yes, also return a dict with None values, and maybe emit a warn

    def save_metadata(self, metadata):
        metadata = Base64Serializer.serialize(metadata)

        query = (("COMMENT ON {} {} IS '{}';"
                  .format(self._identifier.kind,
                          self._identifier,
                          metadata)))

        cur = self.client.connection.cursor()
        cur.execute(query)
        self.client.connection.commit()
        cur.close()

    def exists(self):
        cur = self.client.connection.cursor()

        if self._identifier.schema:
            schema = self._identifier.schema
        else:
            # if schema is empty, we have to find out the default one
            query = """
            SELECT replace(setting, '"$user", ', '')
            FROM pg_settings WHERE name = 'search_path';
            """
            cur.execute(query)
            schema = cur.fetchone()[0]

        # https://stackoverflow.com/a/24089729/709975
        query = """
        SELECT EXISTS (
            SELECT 1
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = %(schema)s
            AND    c.relname = %(name)s
        );
        """

        cur.execute(query, dict(schema=schema,
                                name=self._identifier.name))
        exists = cur.fetchone()[0]
        cur.close()
        return exists

    def delete(self, force=False):
        """Deletes the product
        """
        cascade = 'CASCADE' if force else ''
        query = ("DROP {} IF EXISTS {} {}"
                 .format(self._identifier.kind, self, cascade))
        self.logger.debug('Running "%s" on the databse...', query)

        cur = self.client.connection.cursor()
        cur.execute(query)
        cur.close()
        self.client.connection.commit()

    @property
    def name(self):
        return self._identifier.name

    @property
    def schema(self):
        return self._identifier.schema

    @property
    def kind(self):
        return self._identifier.kind
