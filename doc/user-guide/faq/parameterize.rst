Parameterizing Notebooks
------------------------
When performing a notebook task, you must first parametize the notebook by assigning the tag ``parameters`` to an initial cell. 
These ``parameters`` are then used in the workflow when the notebook is executed.

Python Files (.py)
------------------
For py files, include the ``# + tags["parameters]`` comment before declaring your default variables or parameters.
The ``# +`` marker denotes the beginning of a cell and ``# -`` marker indicates the end of the cell.

.. image:: /doc/_static/img/parameterize-py-example.png
  :width: 700

JupterLab 3.0+
--------------
If you are using JupterLab version 3 or above, select the cell to parameterize. Then in the right sidebar, click to open the property inspector (double gear icon).
Hit the "Add Tag" button, type in the word ``parameters``, and press "Enter".

For more information on parameterizing notebooks in older verions, please refer to the papermill docs: `parameterize <https://papermill.readthedocs.io/en/stable/usage-parameterize.html>`_