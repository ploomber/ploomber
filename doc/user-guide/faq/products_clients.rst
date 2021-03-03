Why do products have clients?
-----------------------------

Clients exist in tasks and products because they serve different purposes. A
task client handles the communication to a system where the source code will
be executed. On the other hand, product's client only handles the product's
metadata.

To enable incremental runs. Ploomber has to store the source code that generated
any given product. To make this simpler, metadata may be stored in the
same system, but doing so requires a system-specific implementation.
Currently, only SQLite and PostgreSQL are supported via
:py:mod:`ploomber.products.SQLiteRelation` and
:py:mod:`ploomber.products.PostgresRelation` respectively. For these two cases
task client and product client communicate to the same system (the db), hence
they can be initialized with the same client.

For any other database, we provide two alternatives, in both cases, the
task's client is different from the product's client. The first alternative
is :py:mod:`ploomber.products.GenericSQLRelation` which represents a generic
table or view and saves metadata in a SQLite database, on this case, the
task's client is the database client (e.g. Oracle, Hive, Snowflake) but
the product's client is a SQLite client. If you don't need the incremental
builds features, you can use :py:mod:`ploomber.products.SQLRelation` instead
which is a product with no metadata.
