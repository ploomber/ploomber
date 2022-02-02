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

.. code-block:: console
    # ML pipeline example
    ploomber examples -n templates/ml-basic -o ml-basic
    cd ml-basic

.. code-block:: console
    # install dependencies
    pip install -r requirements.txt

What we're doing in this section is getting a preconfigured workflow. You can
review the code in your ``local``.

Run ploomber
------------
.. code-block:: console
    # run pipeline (output will be available in the output directory)
    ploomber build

We just ran the ploomber pipeline, you can change parts of the code and see how it affects *execution time*.
You should go to the ``outputs`` folder that was created and open the HTML report to see the execution results.

What's next?
************

Depends what you want to achieve:
* **Bring your own code!** Check out the tutorial to migrate your code to Ploomber :doc:`../user-guide/refactoring`. <br>
* You can go deeper and build your first python pipeline guide: :doc:`../get-started/spec-api-python`. <br>

You can run **another example** as well