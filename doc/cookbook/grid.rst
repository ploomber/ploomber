Task grids
==========

You can use ``tasks[*].grid`` to create multiple tasks from a single task declaration, for example, to train various models with different parameters and run them in parallel.

.. code-block:: yaml
    :class: text-editor

    # execute independent tasks in parallel
    executor: parallel

    tasks:
      - source: random-forest.py
        # generates random-forest-1, random-forest-2, ..., random-forest-6
        name: random-forest-
        product: random-forest.html
        grid:
            # creates 6 tasks (3 * 2)
            n_estimators: [5, 10, 20]
            criterion: [gini, entropy]


Click here to see the complete `example <https://github.com/ploomber/projects/tree/master/cookbook/grid>`_.

Click here to go to the ``grid`` API  :ref:`documentation <tasks-grid>`.

An in-depth tutorial showing how to use ``grid`` and MLflow for experiment tracking is `available here <https://github.com/ploomber/projects/blob/master/templates/mlflow/README.ipynb>`_.