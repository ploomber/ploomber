Interactive sessions
--------------------

To start an interactive session:

.. code-block:: console

    ipython -i -m ploomber.entry pipeline.yaml -- --action status

Once the interactive session opens, use the ``dag`` object.


Visualize dependencies:

.. code-block:: python
    :class: ipython

    dag.plot()

Develop scripts interactively:

.. code-block:: python
    :class: ipython

    dag['some_task'].develop()


Line by line debugging:

.. code-block:: python
    :class: ipython

    dag['some_task'].debug()


Print the rendered source code from SQL scripts:


.. code-block:: python
    :class: ipython

    print(dag['some_sql_task'].source)
