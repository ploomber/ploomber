Refactoring legacy notebooks
============================

This tutorial shows how to convert legacy notebooks into Ploomber pipelines.

.. note::

    If you don't have a sample notebook, download one from
    `here <https://github.com/ploomber/soorgeon/blob/main/examples/machine-learning/nb.ipynb>`_.


    or execute:

    .. code-block:: console

        curl -O https://raw.githubusercontent.com/ploomber/soorgeon/main/examples/machine-learning/nb.ipynb


The only requirement for your notebook is to separate
sections with H2 headings:

.. image:: https://ploomber.io/images/doc/h2-heading.png
   :target: https://ploomber.io/images/doc/h2-heading.png
   :alt: h2-heading


Here's an example notebook with three sections separated by H2 headings:


.. image:: https://ploomber.io/images/doc/nb-with-h2-headings.png
   :target: https://ploomber.io/images/doc/nb-with-h2-headings.png
   :alt: nb-with-h2-headings


Once your notebook is ready, you can refactor it with:

.. code-block:: bash

    # install soorgeon
    pip install soorgeon 

    # refactor the nb.ipynb notebook
    soorgeon refactor nb.ipynb


.. tip::
    
    Sometimes, ``soorgeon`` may not be able to split your
    notebook sections, if so, run ``soorgeon refactor nb.ipynb --single-task``
    to generate a pipeline with one task. If you have questions, send us a
    `message on Slack <https://ploomber.io/community>`_.

The command above will generate a ``pipeline.yaml`` with your pipeline
declaration and ``.ipynb`` tasks (one per section).

You can also tell Soorgeon to generate tasks in ``.py`` format:

.. code-block:: bash

    # generate tasks in .py format (requires soorgeon>=0.0.13)
    soorgeon refactor nb.ipynb --file-format py


Note that due to the
:doc:`Jupyter integration <jupyter>`, **you can open .py files as notebooks in Jupyter**

.. image:: https://ploomber.io/images/doc/lab-open-with-notebook.png
   :target: https://ploomber.io/images/doc/lab-open-with-notebook.png
   :alt: lab-open-with-notebook

To run the pipeline:

.. code-block:: bash

    # install dependencies
    pip install -r requirements.txt

    # run Ploomber pipeline
    ploomber build


That's it! Now that you have a Ploomber pipeline, you can benefit from all
our features! If you want to learn more about the framework, check out the :doc:`basic concepts tutorial <../get-started/basic-concepts>`.

Resources
---------

* `Soorgeon's user guide <https://github.com/ploomber/soorgeon/blob/main/doc/guide.md>`_
* `GitHub <https://github.com/ploomber/soorgeon>`_
* `Interactive example <https://github.com/ploomber/projects/tree/master/guides/refactor>`_
* Blog post series on notebook refactoring: `Part I <https://ploomber.io/blog/refactor-nb-i/>`_, and `Part II <https://ploomber.io/blog/refactor-nb-ii/>`_