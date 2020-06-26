Which databases are supported?
------------------------------

The answer depends on the task to use. interact with databases via clients,
there are two clients available. If the database you want
to use is supported by SQLAlchemy, you can use :py:mod:`ploomber.clients.SQLAlchemyClient`, if the database has a client that
implements Python's Database API Specification (`PEP 249 <https://www.python.org/dev/peps/pep-0249/>`_), you can use :py:mod:`ploomber.clients.DBAPIClient`.

:py:mod:`ploomber.tasks.SQLDump` supports both types of clients, you should
be able to dump data to local files from pretty much all databases.

:py:mod:`ploomber.products.SQLScript` supports both types of clients but since
it is intended to create new tables/views in the database, the product also
needs a client. See the answer above for details.

:py:mod:`ploomber.tasks.SQLUpload` relies on `pandas.to_sql` to upload a local
file to a database. Such method relies on SQLAlchemy to work, hence it only
supports ``SQLAlchemyClient``.

:py:mod:`ploomber.tasks.PostgresCopyFrom` is a faster alternative to
``SQLUpload`` when using PostgreSQL. It relies on `pandas.to_sql` **only**
to create the database, but actual data upload is donce using ``psycopg``
which calls the native ``COPY FROM`` procedure.
