"""
A generic product whose metadata is saved in a given directory and
exists/delete methods are bash commands
"""
from ploomber.products.product import Product
from ploomber.products.mixins import SQLProductMixin, ProductWithClientMixin
from ploomber.products.sql import SQLiteBackedProductMixin
from ploomber.placeholders.placeholder import Placeholder, SQLRelationPlaceholder


# TODO: add check_product and run tests: e.g .name should return a string
# no placeholders objects or {{}}
class GenericProduct(SQLiteBackedProductMixin, ProductWithClientMixin, Product):
    """
    GenericProduct is used when there is no specific Product implementation.
    Sometimes it is technically possible to write a Product implementation
    but if you don't want to do it you can use this one. Other times is is
    not possible to provide a concrete Product implementation (e.g. we
    cannot store arbitrary metadata in a Hive table). GenericProduct works
    as any other product but its metadata is stored not in the Product itself
    but in a different backend.

    Parameters
    ----------
    identifier : str
        An identifier fot this product, can contain placeholders
        (e.g. {{placeholder}})

    client : ploomber.clients.DBAPIClient or SQLAlchemyClient, optional
        The client used to *store metadata for this product*. Only required
        if no dag-level client has been declared using dag.clients[class]

    Notes
    -----
    exists does not check for product existence, just checks if metadata exists
    delete does not perform actual deletion, just deletes metadata
    """

    def __init__(self, identifier, client=None):
        super().__init__(identifier)
        self._client = client

    def _init_identifier(self, identifier):
        return Placeholder(identifier)

    def exists(self):
        # just check if there is metadata
        return self.fetch_metadata() is not None

    def delete(self, force=False):
        """Deletes the product"""
        # just delete the metadata, we cannot do anything else
        return self._delete_metadata()

    @property
    def name(self):
        return str(self._identifier)


class GenericSQLRelation(SQLProductMixin, GenericProduct):
    """
    A GenericProduct whose identifier is a SQL relation, uses SQLite as
    metadata backend

    Parameters
    ----------
    identifier : tuple of length 3
        A tuple with (schema, name, kind) where kind must be either 'table'
        or 'view'

    client : ploomber.clients.DBAPIClient or SQLAlchemyClient, optional
        The client used to *store metadata for this product*. Only required
        if no dag-level client has been declared using dag.clients[class]

    See Also
    --------
    ploomber.products.SQRelation :
        SQL relation (table or view) with no metadata.
    """

    def _init_identifier(self, identifier):
        return SQLRelationPlaceholder(identifier)

    @property
    def name(self):
        return self._identifier.name

    @property
    def schema(self):
        return self._identifier.schema

    @property
    def kind(self):
        return self._identifier.kind

    def __repr__(self):
        return f"{type(self).__name__}({self._identifier._raw_repr()})"

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))


class SQLRelation(SQLProductMixin, Product):
    """
    A product that represents a SQL relation (table or view) with no metadata
    (incremental builds won't work). See
    :class:`ploomber.products.GenericSQLRelation` if you want to enable
    incremental builds.

    Parameters
    ----------
    identifier : tuple of length 3
        A tuple with (schema, name, kind) where kind must be either 'table'
        or 'view'

    See Also
    --------
    ploomber.products.GenericSQLRelation :
        SQL relation (table or view) that stores metadata (to enable
        incremental builds) in a SQLite database.
    """

    def _init_identifier(self, identifier):
        return SQLRelationPlaceholder(identifier)

    @property
    def name(self):
        return self._identifier.name

    @property
    def schema(self):
        return self._identifier.schema

    @property
    def kind(self):
        return self._identifier.kind

    def fetch_metadata(self):
        return None

    def save_metadata(self, metadata):
        pass

    def exists(self):
        return True

    def delete(self, force=False):
        pass

    def __repr__(self):
        return f"{type(self).__name__}({self._identifier._raw_repr()})"

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))
