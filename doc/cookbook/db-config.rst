Database configuration
======================

To have SQL scripts as tasks, you must configure a database client. There are
two available clients: :py:mod:`ploomber.clients.SQLAlchemyClient` and
:py:mod:`ploomber.clients.DBAPIClient`, we recommend using the sqlalchemy
client if your database is supported because it is compatible with more types
of SQL tasks (e.g., :py:mod:`ploomber.tasks.SQLDump`, which dumps data into
a local file).

Using ``SQLAlchemyClient``
--------------------------

Ensure that you can connect to the database using ``sqlalchemy``:

.. code-block:: python
    :class: text-editor

    from sqlalchemy import create_engine

    engine = create_engine('DATABASE_URI')

``DATABASE_URI`` depends on the type of database. sqlalchemy
supports a wide range of databases; you can find a list in
their `documentation <https://docs.sqlalchemy.org/en/14/core/engines.html#supported-databases>`_,
while others come in third-party packages (e.g., `Snowflake <https://docs.snowflake.com/en/user-guide/sqlalchemy.html>`_).


If ``create_engine`` is successful, ensure you can query your database:

.. code-block:: python
    :class: text-editor

    with engine.connect() as conn:
        results = conn.execute('SELECT * FROM some_table LIMIT 10')

If the query works, you can initialize a SQLAlchemyClient with the same
``DATABASE_URI``:

.. code-block:: python
    :class: text-editor

    from ploomber.clients import SQLAlchemyClient

    client = SQLAlchemyClient('DATABASE_URI')


Using Snowflake
***************

Here's some sample code to configure Snowflake:

.. code-block:: sh

    # install snowflake-sqlalchemy
    pip install snowflake-sqlalchemy


Build your URL with the helper function:

.. code-block:: python
    :class: text-editor

    from snowflake.sqlalchemy import URL

    params = dict(user='user',
                  password='pass',
                  account='acct',
                  warehouse='warehouse',
                  database='db',
                  schema='schema',
                  role='role')
    
    client = SQLAlchemyClient(URL(**params))

If using OAuth instead of user/password authentication, you need to include the
token:

.. code-block:: python
    :class: text-editor

    import json
    import requests # pip install requests
    from snowflake.sqlalchemy import URL

    def get_snowflake_token(username, password, account):
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'password',
            'scope': 'SESSION:ROLE-ANY',
            'username: username,
            'password': password,
            'client_id: f'https://{account}.snowflakecomputing.com',
        }
        response = requests.post(oauth_url, data=data, headers=headers,
                                 verify=False)
        
        return str(json.loads(response.text)['access_token']).strip()
    
    token = get_snowflake_token('user', 'password', 'account')

    params = dict(user='user',
                  account='acct',
                  warehouse='warehouse',
                  database='db',
                  schema='schema',
                  role='role',
                  authentication='oauth',
                  token=token)
    
    client = SQLAlchemyClient(URL(**params))



Using ``DBAPIClient``
---------------------

DBAPIClient takes a function that returns a
`DBAPI <https://www.python.org/dev/peps/pep-0249/>`_ compatible connection
and parameters to initialize such connection.

Here's an example with SQLite:


.. code-block:: python
    :class: text-editor

    from ploomber.clients import DBAPIClient
    import sqlite3

    client = DBAPIClient(sqlite3.connect, dict(database='my.db'))


Under the hood, Ploomber calls ``sqlite3.connect(database='my.db')``.

Another example, this time using `Snowflake <https://docs.snowflake.com/en/user-guide/python-connector-example.html#setting-session-parameters>`_:

.. code-block:: python
    :class: text-editor

    from ploomber.clients import DBAPIClient
    import snowflake.connector

    params = dict(user='USER', password='PASS', account='ACCOUNT')
    client = DBAPIClient(snowflake.connector.connect, params)


Configuring the client in ``pipeline.yaml``
-------------------------------------------

Check out the :doc:`../get-started/sql-pipeline` to learn how to configure
the database client in your ``pipeline.yaml`` file. 

Examples
--------

To see some examples using SQL connections, see this:

1. `A short example that dumps data <https://github.com/ploomber/projects/tree/master/cookbook/sql-dump>`_.
2. `A SQL pipeline <https://github.com/ploomber/projects/tree/master/templates/spec-api-sql>`_.
3. `An ETL pipeline  <https://github.com/ploomber/projects/tree/master/templates/etl>`_.