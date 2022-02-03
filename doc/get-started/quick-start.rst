Installation
------------

Using ``pip``
*************

.. code-block:: console

    pip install ploomber


Using ``conda``
***************

.. code-block:: console

    conda install ploomber -c conda-forge



Get an example
--------------

What we're doing in this section is getting a preconfigured workflow (example).
You can review the code in your ``local``.

Then we install the dependencies.

.. code-block:: console

    ploomber examples -n templates/ml-basic -o ml-basic
    cd ml-basic

.. code-block:: console

    pip install -r requirements.txt

Run ploomber
------------
.. code-block:: console

    ploomber build

We just ran the ploomber pipeline, you can change parts of the code and see how it affects *execution time*.
You should go to the ``outputs`` folder that was created and open the HTML report to see the execution results.

What's next?
************

Depends what you want to achieve:

* **Bring your own code!** Check out the tutorial to migrate your code into Ploomber: :doc:`../user-guide/refactoring`.

* You can go deeper and build your first python pipeline guide: :doc:`../get-started/spec-api-python`.

You can run **another example** as well