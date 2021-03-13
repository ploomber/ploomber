Which databases are supported?
------------------------------

The answer depends on the task to use. There are two types of database clients.
:py:mod:`ploomber.clients.SQLAlchemyClient` for ``SQLAlchemy`` compatible
database and :py:mod:`ploomber.clients.DBAPIClient` for the rest (the only
requirement for ``DBAPIClient`` is a driver that implements
`PEP 249 <https://www.python.org/dev/peps/pep-0249/>`_.

:py:mod:`ploomber.tasks.SQLDump` supports both types of clients.

:py:mod:`ploomber.tasks.SQLScript` supports both types of clients. But if you
want incremental builds, you must also configure a product client. See the section
below for details.

:py:mod:`ploomber.tasks.SQLUpload` relies on `pandas.to_sql` to upload a local
file to a database. Such method relies on SQLAlchemy to work. Hence it only
supports ``SQLAlchemyClient``.

:py:mod:`ploomber.tasks.PostgresCopyFrom` is a faster alternative to
``SQLUpload`` when using PostgreSQL. It relies on `pandas.to_sql` **only**
to create the database, but actual data upload is done using ``psycopg``
which calls the native ``COPY FROM`` procedure.
