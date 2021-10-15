Logging
=======

.. note:: This is a quick reference, for an in-depth tutorial, :doc:`click here <../user-guide/logging>`.

Function tasks
--------------

If you're using functions as tasks, configure logging like this:

.. code-block:: python
    :class: text-editor

    import logging

    def some_task(product):
        # uncomment the next line if using Python >= 3.8 on macOS
        # logging.basicConfig(level=logging.INFO)

        logger = logging.getLogger(__name__)

        # to log a message, call logger.info
        logger.info('Some message')

Scripts or notebooks
--------------------

If using scripts/notebooks tasks, add this a the top of **each** one:


.. code-block:: python
    :class: text-editor

    import sys
    import logging

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    logger = logging.getLogger(__name__)

    # to log a message, call logger.info
    logger.info('Some message')


**and** add the following to each task definition:


.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
      - source: scripts/script.py
        product: products/output.ipynb
        # add this
        papermill_params:
          log_output: True


Then, use the ``--log`` option when building the pipeline to print records to the terminal:

.. code-block:: console

    ploomber build --log info


If you also want to send logs to a file:


.. code-block:: console

    ploomber build --log info --log-file my.log
