Task grids
==========

You can use ``tasks[*].grid`` to create multiple tasks from a single task
declaration, for example, to train various models with different parameters:

.. raw:: html

    <div class="mermaid">
    graph LR
        load[Load] --> process[Process] --> exp1[Train n_estimators=5, criterion=gini]
        process --> exp2[Train n_estimators=10, criterion=gini]
        process --> exp3[Train n_estimators=20, criterion=gini]
        process --> exp4[Train n_estimators=5, criterion=entropy]
        process --> exp5[Train n_estimators=10, criterion=entropy]
        process --> exp6[Train n_estimators=20, criterion=entropy]

    </div>


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


Download example:

.. code-block:: sh
    :class: text-editor

    pip install ploomber
    ploomber examples -n cookbook/grid -o grid
    cd grid
    pip install -r requirements.txt
    ploomber build

Click here to see the complete `example <https://github.com/ploomber/projects/tree/master/cookbook/grid>`_.

For full details, see the ``grid`` API  :ref:`documentation <tasks-grid>`.

An in-depth tutorial showing how to use ``grid`` and MLflow for experiment tracking is `available here <https://github.com/ploomber/projects/blob/master/templates/mlflow/README.ipynb>`_.