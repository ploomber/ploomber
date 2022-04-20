.. _parametrizing-notebooks:

Parameterizing Notebooks
------------------------

You must first parametrize the notebook by assigning the tag ``parameters`` to an
initial cell when performing a notebook task. Note that the parameters in 
the ``parameters`` cell are placeholders; they indicate the parameter names that
your script or notebook takes, but they are replaced values declared in
your ``pipeline.yaml`` file at runtime. The only exception is
the ``upstream`` parameter, which contains a list of task dependencies.

Parameterizing ``.py`` files
*****************************

For ``.py`` files, include the ``# %% tags=["parameters"]`` comment before declaring your default variables or parameters.


.. code-block:: python
    :class: text-editor

    # %% tags=["parameters"]
    upstream = None
    product = None

Note that Ploomber is compatible with all ``.py`` formats supported by jupytext. Another common alternative is the light format.
The ``# +`` marker denotes the beginning of a cell, and ``# -`` marker indicates the end of the cell. Your cell should look like this:


.. code-block:: python
    :class: text-editor

    # + tags=["parameters"]
    upstream = None
    product = None
    # -

If you're using another format, check out `jupytext's documentation <https://jupytext.readthedocs.io/en/latest/formats.html>`_.

Parameterizing ``.ipynb`` files in Jupyter
******************************************

.. note:: This applies to JupyterLab 3.0 and higher. For more information on parameterizing notebooks in older versions, please refer to `papermill docs <https://papermill.readthedocs.io/en/stable/usage-parameterize.html>`_

To parametrize your notebooks, add a new cell at the top, then in the right sidebar, click to open the property
inspector (double gear icon). Next, hit the "Add Tag" button, type in the word ``parameters``, and press "Enter".

.. image:: /_static/img/parameterize-ipynb-example.png
  :width: 800
