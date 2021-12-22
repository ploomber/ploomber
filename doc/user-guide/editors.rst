Other editors (VSCode, PyCharm, etc.)
=====================================

Although Ploomber can be entirely operated from the command-line, thus,
independent of your text editor or IDE of choice, Ploomber comes with a
Jupyter plugin that streamlines interactive development via the cell injection
process.

Whenever you open a script or notebook, Ploomber extracts the upstream
dependencies and creates a new cell with the location of the inputs for the
current wask; this allows you to interactively develop your pipeline without
hardcoding paths. To learn mode about cell injection, :doc:`click here <jupyter>`. 

Depending on your text editor/IDE capabilities, you may choose one of two
options:

1. Use the ``percent`` format in ``.py`` files (recommended)
2. Pair ``.py`` files with ``.ipynb`` files (recommended if your editor does not support the ``percent`` format)
3. Use ``.ipynb`` files as sources

Using the ``percent`` format
----------------------------

The percent format separates cells using ``# %%``. Editors such as **VSCode,
PyCharm, Spyder, and Atom (via Hydrogen)** support this.

The first step is to ensure that your scripts are in the percent format, you
can re-format all of them with the following command:

.. code-block:: console

    ploomber nb --format py:percent


.. _manual-cell-injection:

Now, let's inject the cell into each script manually:

.. code-block:: console

    ploomber nb --inject

If you open any of your pipeline scripts, you'll see the injected cell.

.. important::

    Remember to run ``ploomber nb --inject`` if you edit your ``pipeline.yaml``.


.. note::

    By default, Ploomber deletes the injected cell when you save a
    script/notebook from Jupyter, however, if you injected it via the
    ``ploomber nb --inject`` command, this is disabled, and saving the
    script/notebook will not remove the injected cell.

Pairing ``.ipynb`` files
------------------------

**If your editor does not support developing .py files interactively**,
you can pair ``.py`` files with ``.ipynb`` ones: this creates a ``.ipynb``
copy of each ``.py`` task, and whenever you modify the ``.ipynb`` one, the
``.py`` copy can be synced with a simple command.

To create the ``.ipynb`` files:

.. code-block:: console

    ploomber nb --pair notebooks


The command above will generate ``.ipynb`` in a ``notebooks/`` directory, one
per ``.py`` in your pipeline.

To add the injected cell, follow the instructions from the :ref:`previous section <manual-cell-injection>`.

.. tip::

    Keep your repository clean by adding the ``.ipynb`` files to your
    ``.gitignore`` file.


Once you modify the  ``.ipynb``, you can sync their  ``.py`` counterparts with:

.. code-block:: console

    ploomber nb --sync


Using ``.ipynb`` as sources
---------------------------

As a last option, you have the option to use ``.ipynb`` files as task sources
in your ``pipeline.yaml``:


.. code-block:: yaml
    :class: text-editor
    
    tasks:
      - source: nbs/load.ipynb
        product: output/report.ipynb 


Keep in mind that ``.ipynb`` files are hard to manage with with, so we recommend
you to use one of the alternative options described above.

To add the injected cell, follow the instructions from the :ref:`previous section <manual-cell-injection>`.

Removing the injected cell
--------------------------

If you wish to remove the injected cell:

.. code-block:: console

    ploomber nb --remove


Using ``git`` hooks
-------------------

Since the injected cell depends on your ``pipeline.yaml`` (and your
``env.yaml``, if you have one), the cell's content may become outdated so it's
a good idea to use it for development purposes only, but do not include them in
the repository.

To automate injecting/removing, you can install you can configure git hooks
to automatically remove the injected cells before committing files and inject
them again after storing the commit:

.. code-block:: console

    ploomber nb --install-hook


To uninstall the hooks:

.. code-block:: console

    ploomber nb --uninstall-hook