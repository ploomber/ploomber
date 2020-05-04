Supported Databases
-------------------

tl; dr; ``SQLDump`` (dump data from a db to a local file) supports most databases, for other tasks,
SQLite and PostgreSQL are supported, other databases can be used by using ``GenericSQLRelation``.

Ploomber Tasks and Products interact with databases via clients, there are two
clients available. If the database you want
to use is supported by SQLAlchemy, you can use :py:mod:`ploomber.clients.SQLAlchemyClient`, if the database has a client that
implements Python's Database API Specification (`PEP 249 <https://www.python.org/dev/peps/pep-0249/>`_), you can use :py:mod:`ploomber.clients.DBAPIClient`.

This covers most use cases to dump data
using :py:mod:`ploomber.tasks.SQLDump`. If none of that works, you
can always use :py:mod:`ploomber.tasks.PythonCallable`, where you can define
your own database communication logic in a function.

To keep track of source code changes, ploomber stores metadata on each Product,
which means we have to code database-specific logic.
:py:mod:`ploomber.products.SQLiteRelation` stores metadata by creating a table
called `_metadata`, while  :py:mod:`ploomber.products.PostgresRelation` stores metadata using ``COMMENT ON`` queries. For other types of databases, a :py:mod:`ploomber.products.GenericSQLRelation` can be used, ``GenericSQLRelation`` also stores metadata in a SQLite database.

For example, you can use ``GenericSQLRelation`` to interface with systems such as Hive, metadata
will not be stored in Hive tables but in a separate SQLite database.

Apart from saving metadata, Products also implement ``Product.exists()`` and
``Product.delete()`` methods, for ``SQLiteRelation`` and ``PostgresRelation`` these
two methods send custom queries. Since ``GenericSQLRelation`` has no way
of knowing how to send a command to a generic database to check if a table/view exists or to delete it,
``GenericSQLRelation`` will just check if metadata exists (``Product.exists()``) or delete it (``Product.delete()``).

If you don't want to use ``GenericSQLRelation``, you can subclass ``Product`` and
implement methods for checking products, deleting them, storing and retrieving metadata.
