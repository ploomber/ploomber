Research Projects
=================

Ploomber can help you manage your research project to enhance reproducibility
and to run more experiments faster.

Check out our `Exploratory Data Analysis template <https://github.com/ploomber/projects/tree/master/templates/exploratory-analysis>`_ to get started.

Faster iterations
******************

Thanks to :ref:`incremental builds <incremental-builds>`, you can make small changes
to your data analysis code and quickly bring your results up-to-date, since
Ploomber will only execute the code that has changed since your last run.

More experiments
****************

Ploomber allows you to run many experiments efficiently.
You can :doc:`parametrize your pipeline <../user-guide/parametrize>` to run the
same code with different configurations. For example, to run the same analysis
with different data slices. To accelerate your research, you can execute
independent tasks in parallel with no extra configuration.

Ensure reproducibility
**********************

Since Ploomber generates an output notebook (that may include any number of
tables or charts) whenever you execute your pipeline, you can easily bookkeep
the results of each experiment. Whenever you make changes, such executed
notebooks from previous runs can help you verify the reproducibility of your
results.

Share your analysis
*******************

Ploomber can orchestrate all your data analysis for you if you need someone
else to run your code, all they have to do is execute the following command:

.. code-block:: console

    ploomber build
