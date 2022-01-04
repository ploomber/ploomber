Refactoring legacy notebooks
============================

If you already have notebooks that you wish to convert into maintainable Ploomber pipelines,
we developed Soorgeon, a command-line tool that takes a Jupyter notebook as input
and outputs a Ploomber pipeline, that is, a ``pipeline.yaml`` and one ``.py`` file per section
in the notebook.

Here's an example:

.. code-block:: bash

    # install packages
    pip install soorgeon ploomber

    # get the example's code
    git clone https://github.com/ploomber/soorgeon

    # refactor the sample ML notebook
    cd examples/machine-learning
    soorgeon refactor nb.ipynb

    # run the auto-generated Ploomber pipeline
    ploomber build


Soorgeon
--------

To install Soorgeon:

.. code-block:: console

    pip install soorgeon

Running Soorgeon is as simple as:

.. code-block:: console

    soorgeon refactor nb.ipynb

* `Click here <https://github.com/ploomber/soorgeon/blob/main/doc/guide.md>`_ to go to Soorgeon's user guide
* `Soorgeon is available on GitHub <https://github.com/ploomber/soorgeon>`_
* `An interactive example is available here <https://github.com/ploomber/projects/tree/master/guides/refactor>`_


Refactoring guide
-----------------

We've published in our blog a detailed guide to refactoring notebooks:

1. `Part I <https://ploomber.io/blog/refactor-nb-i/>`_
2. `Part II <https://ploomber.io/blog/refactor-nb-ii/>`_