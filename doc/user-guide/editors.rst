Other editors (VSCode, PyCharm, etc.)
=====================================

Although Ploomber can be entirely operated from the command-line, thus,
independent of your text editor or IDE of choice. However, Ploomber comes with a
Jupyter plugin that streamlines development. Whenever you open a script or
notebook, Ploomber extracts the upstream information from your
``pipeline.yaml`` file and creates a new cell with the input paths for the
current task (to learn mode about cell injection, :doc:`click here <jupyter>`).

Depending on your text editor/IDE capabilities, you may choose one of three
options:

1. Use the ``percent`` format in ``.py`` files (recommended)
2. Pair ``.py`` files with ``.ipynb`` files (recommended if your editor does not support the ``percent`` format)
3. Use ``.ipynb`` files as sources

Using the ``percent`` format
----------------------------

.. note::

    Editors such as **VSCode, PyCharm, Spyder, and Atom (via Hydrogen)** support
    the percent format.

The percent format allows you to represent ``.py`` files as notebooks by
separating cells using ``# %%``:

.. code-block:: python
    :class: text-editor

    # %%
    # first cell
    x = 1

    # %%
    # second cell
    y = 2

The first step is to ensure that your scripts are in the percent format. You
can re-format all of them with the following command:

.. code-block:: console

    ploomber nb --format py:percent


.. _manual-cell-injection:

Now, let's inject the cell into each script manually:

.. code-block:: console

    ploomber nb --inject


If you open any of your pipeline scripts, you'll see the injected cell. For
example, say you have a ``clean.py`` file that depends on a ``load.py`` file,
your injected cell will look like this:

.. code-block:: python
    :class: text-editor

    # %% tags=["injected-parameters"]
    upstream = {"raw": "data/raw.csv"}

    product = {
        "nb": "data/clean.html",
        "data": "data/clean.csv",
    }


.. important::

    Remember to run ``ploomber nb --inject`` whenever you change
    your ``pipeline.yaml``.


.. note::

    By default, Ploomber deletes the injected cell when you save a
    script/notebook from Jupyter; however, if you injected it via the
    ``ploomber nb --inject`` command, this is disabled, and saving the
    script/notebook will not remove the injected cell.

Pairing ``.ipynb`` files
------------------------

**If your editor does not support the percent format**,
you can pair ``.py`` and ``.ipynb`` files: this creates a ``.ipynb``
copy of each ``.py`` task, and whenever you modify the ``.ipynb`` one, the
``.py`` syncs.

Say you have a pipeline with ``.py`` files, to create the ``.ipynb`` ones:

.. code-block:: console

    ploomber nb --pair notebooks


The command above will generate ``.ipynb`` files in a ``notebooks/`` directory,
one per ``.py`` in your pipeline.

To add the injected cell, follow the instructions from the
:ref:`previous section <manual-cell-injection>`.

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


Keep in mind that ``.ipynb`` files are hard to manage with git, so we recommend
you to use one of the alternative options described above.

To add the injected cell, follow the instructions from the
:ref:`previous section <manual-cell-injection>`.

Removing the injected cell
--------------------------

If you wish to remove the injected cell from all scripts/notebooks:

.. code-block:: console

    ploomber nb --remove


Using ``git`` hooks
-------------------

.. important::

    ``ploomber nb --install-hook`` does not work on Windows

To keep your scripts/notebooks clean, it's a good idea to keep the injected
cell out of version control.

To automate injecting/removing, you can install git hooks that automatically
remove the injected cells before committing files and inject them again after
committing:

.. code-block:: console

    ploomber nb --install-hook


To uninstall the hooks:

.. code-block:: console

    ploomber nb --uninstall-hook