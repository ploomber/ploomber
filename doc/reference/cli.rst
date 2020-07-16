Command line interface
======================

Entry points: ``ploomber entry``
--------------------------------


Building your pipeline
**********************

Building your pipeline means executing your pipeline end-to-end but speed it up
by skipping tasks whose source code has not changed, this is the most common
command as it takes care of dependencies and brings your pipeline up-to-date:


.. code-block:: console

    ploomber entry pipeline.yaml

Or:

.. code-block:: console

    ploomber entry pipeline.yaml --action build



Interactive sessions
********************


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



Creating new projects: ``ploomber new``
---------------------------------------

