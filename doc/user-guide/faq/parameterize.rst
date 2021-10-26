Parameterizing Notebooks for py files
--------------------------------------
When performing a notebook task, you must first assign the tag ``parameters`` to a cell.
For py files, include the ``# + tags["parameters]`` comment before declaring your variable.
The ``# +`` marker denotes the beginning of a cell and ``# -`` indicates the end of the cell.

.. code-block:: python
    :class: text-editor

    # + tags["parameters"]
    a = 1
    b = 5
    # -

These ``parameters`` are then used when the notebook is executed or run.
For more information on parameterizing notebooks, please refer to the papermill docs: `parameterize <https://papermill.readthedocs.io/en/stable/usage-parameterize.html>`_