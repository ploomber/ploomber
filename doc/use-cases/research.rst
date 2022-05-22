Research Projects
=================

Ploomber can help you manage your research project to enhance reproducibility
and to run more experiments faster. `Click here <https://github.com/ploomber/projects/tree/master/templates/exploratory-analysis>`_ to see a sample project.

.. raw:: html

    <div class="mermaid">
    graph LR
        load[Load data] --> process[Process] --> exp1[Experiment 1]
        process --> exp2[Experiment 2]
        process --> exp3[Experiment 3]
        process --> exp4[Experiment 4]
        exp1 --> summarize[Summarize]
        exp2 --> summarize
        exp3 --> summarize
        exp4 --> summarize
    </div>


Faster iterations
******************

Thanks to :ref:`incremental builds <incremental-builds>`, you can make small changes
to your data analysis code and quickly bring your results up-to-date, since
Ploomber will only execute the code that has changed since your last run.

Run (and organize) more experiments
***********************************

Ploomber allows you to run many experiments in parallel.
You can :doc:`parametrize pipelines <../user-guide/parametrized>` to run the
same code with different configurations.

Furthermore, you can quickly generate all the parameter combinations from a
:doc:`grid <../cookbook/grid>`. If one machine isn't enough, :doc:`export to systems <../deployment/large-scale-training>` like Kubernetes or SLURM easily.

.. collapse:: Example: Running a grid of experiments in parallel

    .. code-block:: console

        pip install ploomber
        ploomber examples -n cookbook/grid -o grid

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
