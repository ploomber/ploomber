Installation
============

Using ``pip``
-------------

.. code-block:: console

    pip install ploomber


To install Ploomber along with all optional dependencies:

.. code-block:: console

    pip install "ploomber[all]"

To plot your pipeline (Optional)
--------------------------------


Install ``pygraphviz`` with conda:

.. code-block:: console

    conda install pygraphviz


``pygraphviz`` depends on another library called ``graphviz``.

``graphviz`` cannot be installed via ``pip``, but you can install it with
``brew``:

.. code-block:: console

    brew install graphviz


Then you can install ``pygraphviz``

.. code-block:: console

    pip install pygraphviz


If you are not using ``conda`` nor ``brew``, check out more options for
installing ``graphviz`` `here <https://www.graphviz.org/download/>`_.
