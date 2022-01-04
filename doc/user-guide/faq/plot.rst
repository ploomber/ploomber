.. _faq-plotting-a-pipeline:

Plotting a pipeline
-------------------

To plot your pipeline using the ``ploomber plot``, you need to
install ``graphviz``, and ``pygraphviz``.

The simplest way to do so is using ``conda``, but you can get it working with ``pip`` too.

``conda`` (simplest)
********************

.. code-block:: console

    conda install pygraphviz -c conda-forge


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

