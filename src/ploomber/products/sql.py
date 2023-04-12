"""
Products representing SQL relations
"""
import abc
import sqlite3
import json

from jinja2 import Template

from ploomber.products import Product
from ploomber.products.mixins import SQLProductMixin, ProductWithClientMixin
from ploomber.products.serializers import Base64Serializer
from ploomber.placeholders.placeholder import SQLRelationPlaceholder


class SQLiteBackedProductMixin(ProductWithClientMixin, abc.ABC):
    """Mixin for products that store metadata in a SQLite database"""

    @property
    @abc.abstractmethod
    def name(self):
        """Used as identifier in the database"""
        pass

    def __create_metadata_relation(self):
        create_metadata = """
        CREATE TABLE IF NOT EXISTS _metadata (
            name TEXT NOT NULL,
            schema TEXT,
            metadata BLOB,
            UNIQUE(schema, name)
        )
        """
        self.client.execute(create_metadata)

    def __get_schema(self):
        # the actual implementation can be a simple identifier or a sql
        # identifier
        if hasattr(self, "schema"):
            # there is a schema (can be None)
            return self.schema
        else:
            # this means there is no schema
            return False

    def fetch_metadata(self):
        self.__create_metadata_relation()

        query = Template(
            """
        SELECT metadata FROM _metadata
        WHERE name = '{{name}}'
        {% if schema is not sameas false %}
            {% if schema %}
                AND schema = '{{schema}}'
            {% else %}
                AND schema IS NULL
            {% endif %}
        {% endif %}
        """
        ).render(name=self.name, schema=self.__get_schema())

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
        self.__create_metadata_relation()

        metadata_bin = json.dumps(metadata).encode("utf-8")
        cur = self.client.connection.cursor()
        schema = self.__get_schema()

        if schema is not False:
            if schema is None:
                cur.execute("DELETE FROM _metadata WHERE name = ?", (self.name,))
            else:
                cur.execute(
                    "DELETE FROM _metadata " "WHERE name = ? AND schema = ?",
                    (self.name, schema),
                )

            # we cannot rely on INSERT INTO since NULL schema values are
            # allowed
            query = """
                INSERT INTO _metadata (metadata, name, schema)
                VALUES(?, ?, ?)
            """
            cur.execute(query, (sqlite3.Binary(metadata_bin), self.name, schema))
        else:
            query = """
                REPLACE INTO _metadata(metadata, name)
                VALUES(?, ?)
            """
            cur.execute(query, (sqlite3.Binary(metadata_bin), self.name))

        self.client.connection.commit()
        cur.close()

    def _delete_metadata(self):
        self.__create_metadata_relation()

        query = Template(
            """
        DELETE FROM _metadata
        WHERE name = '{{name}}'
        {% if schema is not sameas false %}
            {% if schema %}
                AND schema = '{{schema}}'
            {% else %}
                AND schema IS NULL
            {% endif %}
        {% endif %}
        """
        ).render(name=self.name, schema=self.__get_schema())

        cur = self.client.connection.cursor()
        cur.execute(query)
        cur.close()


class SQLiteRelation(SQLProductMixin, SQLiteBackedProductMixin, Product):
    """A SQLite relation

    Parameters
    ----------
    identifier: tuple of length 3 or 2
        A tuple with (schema, name, kind) where kind must be either 'table'
        or 'view'. If passed a tuple with length 2, schema is assumed None.
        Schemas in SQLite represent other databases when using the ATTACH
        command.
    client: ploomber.clients.DBAPIClient or SQLAlchemyClient, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]


    Examples
    --------
    >>> from ploomber.products import SQLiteRelation
    >>> relation = SQLiteRelation(('schema', 'some_table', 'table'))
    >>> str(relation) # returns qualified name
    'schema.some_table'
    """

    def __init__(self, identifier, client=None):
        super().__init__(identifier)
        self._client = client

    def _init_identifier(self, identifier):
        return SQLRelationPlaceholder(identifier)

    def exists(self):
        query = """
        SELECT name
        FROM sqlite_master
        WHERE type = '{kind}'
        AND name = '{name}'
        """.format(
            kind=self.kind, name=self.name
        )

        cur = self.client.connection.cursor()
        cur.execute(query)
        exists = cur.fetchone() is not None
        cur.close()
        return exists

    def delete(self):
        """Deletes the product"""
        query = "DROP {kind} IF EXISTS {relation}".format(
            kind=self.kind, relation=str(self)
        )
        self.logger.debug('Running "{query}" on the databse...'.format(query=query))
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

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))

    def __repr__(self):
        return f"{type(self).__name__}({self._identifier._raw_repr()})"


# FIXME: self._identifier should not be accessed direclty since it might
# be a placeholder
class PostgresRelation(SQLProductMixin, ProductWithClientMixin, Product):
    """A PostgreSQL relation

    Parameters
    ----------
    identifier: tuple of length 3
        A tuple with (schema, name, kind) where kind must be either 'table'
        or 'view'
    client: ploomber.clients.DBAPIClient or SQLAlchemyClient, optional
        The client used to connect to the database. Only required
        if no dag-level client has been declared using dag.clients[class]

    Examples
    --------
    >>> from ploomber.products import PostgresRelation
    >>> relation = PostgresRelation(('schema', 'some_table', 'table'))
    >>> str(relation) # returns qualified name
    'schema.some_table'
    """

    # FIXME: identifier has schema as optional but that introduces ambiguity
    # when fetching metadata and checking if the table exists so maybe it
    # should be required

    def __init__(self, identifier, client=None):
        super().__init__(identifier)
        self._client = client

    def _init_identifier(self, identifier):
        return SQLRelationPlaceholder(identifier)

    def fetch_metadata(self):
        cur = self.client.connection.cursor()

        if self.schema:
            schema = self.schema
        else:
            # if schema is empty, we have to find out the default one
            query = """
            SELECT current_schema()
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
        cur.execute(query, dict(schema=schema, name=self.name))
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

        query = "COMMENT ON {} {} IS '{}';".format(
            self.kind, self._identifier, metadata
        )

        cur = self.client.connection.cursor()
        cur.execute(query)
        self.client.connection.commit()
        cur.close()

    def exists(self):
        cur = self.client.connection.cursor()

        if self.schema:
            schema = self.schema
        else:
            # if schema is empty, we have to find out the default one
            query = """
            SELECT current_schema()
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

        cur.execute(query, dict(schema=schema, name=self.name))
        exists = cur.fetchone()[0]
        cur.close()
        return exists

    def delete(self, force=False):
        """Deletes the product"""
        cascade = "CASCADE" if force else ""
        query = "DROP {} IF EXISTS {} {}".format(self.kind, self, cascade)
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

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))

    def __repr__(self):
        return f"{type(self).__name__}({self._identifier._raw_repr()})"
