R support
---------

R is officially supported by Ploomber. The same concepts that apply to Python
scripts apply to R scripts. The only difference is how you declare your
parameters:

.. code-block:: R
    :class: text-editor
    :name: task-R

    # + tags=["parameters"]
    upstream = list('one_task', 'another_task')
    product = list(nb='path/to/task.ipynb', some_output='path/to/output.csv')
    # -


Installing the IRkernel
-----------------------

Kernel https://github.com/IRkernel/IRkernel
https://docs.anaconda.com/anaconda/user-guide/tasks/using-r-language/


To allow Jupyter to execute R scripts, you need an R installation and install
IRkernel (R package).

It is a good practice to isolate your environments from one project to another,
to avoid messing with your current R installation (if any), we highly recommend
you to have a local installation for your Ploomber project. The easiest way to
do so is via ``conda``, add the following lines to your ``environment.yaml``:

.. code-block:: yaml
    :class: text-editor
    :name: environment-yml

    name: some_project

    dependencies:
      # dependencies...
      - r-base
      - r-irkernel
      # optionally add r-essentials to install commonly used packages


The update your environment:

.. code-block:: console

    conda env update --file environment.yml  --prune


.. code-block:: console

    type R


.. code-block:: r

    echo "IRkernel::installspec()" | Rscript -
