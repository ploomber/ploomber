.. _faq-plotting-a-pipeline:

Plotting a pipeline
-------------------

You can generate a plot of your pipeline with ``ploomber plot``. It supports
using D3 and ``pygraphviz`` as backends to create the plot. D3 is the
most straightforward option since it doesn't require any extra dependencies, but
``pygraphviz`` is more flexible and produces a better plot. Once installed,
Ploomber will use ``pygraphviz``, but you can use the ``--backend`` argument
in the ``ploomber plot`` command to switch between ``d3`` and ``pygraphviz``.

The simplest way to install ``pygraphviz`` is to use ``conda``, but you can also get it working with ``pip``.

``conda`` (simplest)
********************

.. code-block:: console

    conda install pygraphviz -c conda-forge


.. important::
    If you're running Python ``3.7.x`` or lower, run: ``conda install 'pygraphviz<1.8' -c conda-forge``

``pip``
*******


``graphviz`` cannot be installed via ``pip``, so you must install it with
another package manager, if you have ``brew``, you can get it with:

.. code-block:: console

    brew install graphviz


.. note:: If you don't have ``brew``, refer to `graphviz docs <https://www.graphviz.org/download/>`_ for alternatives.

Once you have ``graphviz``, you can install ``pygraphviz`` with ``pip``:

.. code-block:: console

    pip install pygraphviz


.. important::
    If you're running Python ``3.7.x`` or lower, run: ``pip install 'pygraphviz<1.8'``
