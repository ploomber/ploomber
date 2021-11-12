Analytics
=========

Ploomber is a fantastic tool for exploring data and generating analytical
reports.

Instead of coding everything in a single notebook (which is difficult to maintain and
collaborate), you can quickly break down your analysis into multiple parts
and use :ref:`incremental builds <incremental-builds>` to rapidly iterate on
your data.

Once your pipeline is ready, you can easily create HTML reports from
scripts/notebooks, change the extension of the task, and Ploomber will
automatically convert the output for you.


.. code-block:: yaml
    :class: text-editor

    tasks:
      - source: tasks/plot.py
        # convert output notebook to html!
        product: output/report.html


Check out our `this template <https://github.com/ploomber/projects/tree/master/templates/exploratory-analysis>`_ to get started.