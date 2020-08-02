Managing project dependencies
=============================

**Note:** *This guide is not about Ploomber, but rather to explain our
recommended way to manage project dependencies.*

To make sure someone else is able to reproduce your results, you have to ensure
that they can install all required packages.

There are a few options to do so, however, we highly recommend you to use
``conda``. ``conda`` is able to handle many dependencies that otherwise
would require a lot of setup to be installed correctly. Even if you use
``conda``, you'll still be able to use ``pip``.

If you want to know more about dependency management, check out
`this post in our blog <https://ploomber.io/posts/python-envs/>`_.

Installing miniconda (aka ``conda``)
------------------------------------

Conda is available for Windows, MacOS and Linux, `click here for installation
instructions <https://conda.io/projects/conda/en/latest/user-guide/install/index.html#regular-installation>`_.


**Important:** Follow instructions for **Miniconda**, not Anaconda.

Once you finish the setup process, verify that ``conda`` was installed
correctly by running:

.. code-block:: console

    conda --help

Declaring dependencies (``environment.yml``)
--------------------------------------------

Once conda is installed, declare dependencies in an ``environment.yml`` file,
here's an example with common dependencies to get you started:

.. code-block:: yaml
    :class: text-editor
    :name: environment-yml

    name: some_project

    dependencies:
      # having a pkg here is the same as "conda install {pkg}"
      - python==3.8
      - pip
      - numpy
      - scipy
      - pandas
      - matplotlib
      - pip:
        # having a package here is the same as "pip install {pkg}"
        - ploomber

For documentation on ``environment.yml``, `click here <https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-file-manually>`_.

Installing dependencies
-----------------------

To create your environment:

.. code-block:: console

    conda env create --file environment.yml

To activate it:

.. code-block:: console

    conda activate {env_name}

If you modify your ``environment.yml`` file, you can update your dependencies
with:


.. code-block:: console

    conda env update --name {env_name} --file environment.yml  --prune


Where ``{env_name}`` is the value for the ``name`` key in your
``environment.yml`` file.


Troubleshooting
---------------

Once your environment is installed, you might run into trouble if you keep
running ``conda install`` and ``pip install`` on it. Whenever you
encounter issues, the safe way to fix them is to modify your
``environment.yml`` and then create the environment again.

To re-create your environment, first move to the base environment (created
by default):

.. code-block:: console

    conda activate


Then install your environment with the ``--force`` option to replace the old
one:


.. code-block:: console

    conda env create --file environment.yml --force