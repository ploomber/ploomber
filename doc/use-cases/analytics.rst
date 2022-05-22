Analytics
=========

Ploomber is a fantastic tool for data manipulation and generating analytical
reports.

.. raw:: html

    <div class="mermaid">
    graph LR
        la[Load dataset A] --> ca[Clean dataset A]
        lb[Load dataset B] --> cb[Clean dataset B]
        ca --> m[Merge] --> p[Generate report]
        cb --> m
    </div>


.. collapse:: Example: BigQuery and Cloud Storage pipeline

    .. code-block:: console

        pip install ploomber
        ploomber examples -n templates/google-cloud -o google-cloud


.. collapse:: Example: Exploratory data analysis pipeline

    .. code-block:: console

        pip install ploomber
        ploomber examples -n templates/exploratory-analysis -o exploratory-analysis


Modularize your project
***********************

Instead of coding everything in a single notebook (which is difficult to maintain and
collaborate), you can quickly break down your analysis into multiple parts.


Faster iterations
*****************

Finding data insights is an iterative process, with
Ploomber's :ref:`incremental builds <incremental-builds>` you can rapidly
iterate on your data since the framework skips redundant computations and
only executes tasks whose source code has changed since the last execution.


Automated report generation
***************************

Once your pipeline is ready, you can easily create HTML reports from your
scripts/notebooks. Just change the extension of the task, and Ploomber will
automatically convert the output for you.


.. code-block:: yaml
    :class: text-editor

    tasks:
      - source: tasks/plot.py
        # execute your .py file and generate an .html version of it
        # all tables and charts are included
        product: output/report.html
