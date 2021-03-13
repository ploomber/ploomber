Why do products have clients?
-----------------------------

Clients exist in tasks and products because they serve different purposes. A
task client manages the connection to the database that runs your script. On
the other hand, the product's client only handles the storage of the
product's metadata.

To enable incremental runs. Ploomber has to store the source code that generated
any given product. Storing metadata in the same database that runs your code
requires a system-specific implementation. Currently, only SQLite and PostgreSQL
are supported via :py:mod:`ploomber.products.SQLiteRelation` and
:py:mod:`ploomber.products.PostgresRelation` respectively. For these two cases,
task client and product client communicate to the same system (the database).
Hence they can initialize with the same client.

For any other database, we provide two alternatives; in both cases, the
task's client is different from the product's client. The first alternative
is :py:mod:`ploomber.products.GenericSQLRelation` which represents a generic
table or view and saves metadata in a SQLite database; on this case, the
task's client is the database client (e.g., Oracle, Hive, Snowflake) but
the product's client is a SQLite client. If you don't need the incremental
builds features, you can use :py:mod:`ploomber.products.SQLRelation` instead
which is a product with no metadata.
