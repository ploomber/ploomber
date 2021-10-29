Parameterizing Notebooks
------------------------
When performing a notebook task, you must first parametize the notebook by assigning the tag ``parameters`` to an initial cell. 
These ``parameters`` are then used in the workflow when the notebook is executed.

Python Files (.py)
------------------
For py files, include the ``# + tags["parameters]`` comment before declaring your default variables or parameters.
The ``# +`` marker denotes the beginning of a cell and ``# -`` indicates the end of the cell.

.. code-block:: python
    :class: text-editor

    # + tags["parameters"]
    upstream = None
    product = None
    # -

JupterLab 3.0+
--------------
For more information on parameterizing notebooks, please refer to the papermill docs: `parameterize <https://papermill.readthedocs.io/en/stable/usage-parameterize.html>`_