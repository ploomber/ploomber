R support
=========

Ploomber officially supports R. The same concepts that apply to Python
scripts apply to R scripts; this implies that R scripts can render as notebooks
in Jupyter and the cell injection works. The only difference is how
to declare ``upstream`` dependencies:

For the R Markdown format (``.Rmd``):

.. code-block:: md
    :class: text-editor
    :name: task-Rmd

    ```{r, tags=c("parameters")}
    upstream = list('one_task', 'another_task')
    ```


If you prefer, you can also use plain R scripts:

.. code-block:: R
    :class: text-editor
    :name: task-R

    # %% tags=["parameters"]
    upstream = list('one_task', 'another_task')
    #   


If your script doesn't have dependencies: ``upstream = NULL``

To read more about how Ploomber executes scripts and integrates with Jupyter,
check the :doc:`Jupyter Integration guide <../user-guide/jupyter>`.

Configuring R environment
-------------------------

To run R scripts as Jupyter notebooks, you need to install Jupyter first, have 
an existing R installation and install the IRkernel package.

If you are using ``conda`` and a ``environment.yml`` file to manage
dependencies, keep on reading. Otherwise, read the `IRkernel installation
instructions <https://github.com/IRkernel/IRkernel>`_.


Setting up R and IRkernel via ``conda``
---------------------------------------

Even if you already have R installed, it is good to isolate your
environments from one project to another. ``conda`` can install R inside your
project's environment.

Add the following lines to your ``environment.yaml``:

.. code-block:: yaml
    :class: text-editor
    :name: environment-yml

    name: some_project

    dependencies:
      # ...
      # existing conda dependencies...
      - r-base
      - r-irkernel
      # optionally add r-essentials to install commonly used R packages

      - pip:
        # ...
        # existing pip dependencies...
        - ploomber


For more information on installing R via ``conda``
`click here <https://docs.anaconda.com/anaconda/user-guide/tasks/using-r-language/>`_.


Once you update your ``environment.yml``, re-create or update your environment.



Finally, activate the R kernel for Jupyter. If you're using Linux or macOS:

.. code-block:: console

    echo "IRkernel::installspec()" | Rscript -


If using Windows, start an R session and run ``IRkernel::installspec()`` on it.

Interactive example
-------------------

Click the button above to see an interactive example (no installation needed,
but takes about a minute to be ready):

`Example source code <https://github.com/ploomber/projects/tree/master/templates/spec-api-r>`_
