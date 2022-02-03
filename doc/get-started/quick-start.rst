Quickstart
----------

``pip``
*******

.. code-block:: console

    pip install ploomber


``conda``
*********

.. code-block:: console

    conda install ploomber -c conda-forge


Run an example
**************

Download example:

.. code-block:: console

    ploomber examples -n templates/ml-basic -o ml-basic
    cd ml-basic

Install the dependencies:

.. code-block:: console

    pip install -r requirements.txt


Run it:

.. code-block:: console

    ploomber build


You just ran a Ploomber pipeline! 🎉

Check out the ``output`` folder, you'll see an HTML report with model results!

Also, check out the ``pipeline.yaml``, which contains the pipeline declaration.

What's next?
************

* **Bring your own code!** Check out the tutorial to migrate your code into Ploomber: :doc:`../user-guide/refactoring`.
* Check the **introductory tutorial**: :doc:`../get-started/spec-api-python`.
* Run more :doc:`examples <../user-guide/templates>`.