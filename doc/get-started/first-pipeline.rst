
Your first Python pipeline
==========================

This tutorial will guide you to run your first pipeline with Ploomber. You can
either use a terminal from your computer, deepnote (requires free account
but loads faster) or binder:

.. |deepnote| image:: https://deepnote.com/buttons/launch-in-deepnote-small.svg
   :align: middle
   :target: https://deepnote.com/launch?template=deepnote&url=https://github.com/ploomber/projects/blob/master/README.ipynb

.. |binder| image:: https://mybinder.org/badge_logo.svg
   :align: middle
   :target: https://mybinder.org/v2/gh/ploomber/projects/master?filepath=workspace%2FREADME.md

.. table::
   :align: center

   ==========  ======== 
   |deepnote|  |binder|
   ==========  ========

Create sample project
---------------------

Note: if you are using the service from the button above, execute the following
command once the terminal shows up:

.. code-block:: console

    cd workspace

Let's get started, to create a project with basic structure:

.. code-block:: console

    ploomber new


You'll be asked a few questions, for this tutorial answer the following:

1. Project name: ``project``
2. Do you need to connect to a database? ``n``
3. Do you want to use conda? ``n``

Let's take a look at the generated files, you can do so by clicking in the
Jupyter button at the top left corner:

.. image:: https://ploomber.io/doc/ploomber-new/go-to-files.png
    :width: 50%

Then move to the ``workspace/project/`` folder, you should see the following
files:

.. image:: https://ploomber.io/doc/ploomber-new/files.png
    :width: 50%

The ``README.md`` file contains descriptions for each file, feel free to take
a look.

This sample project generates a pipeline with three tasks that have the
following structure:

.. raw:: html

    <div class="mermaid">
    graph LR
        raw.py --> clean.py --> plot.py

        class raw.py pending;
        class clean.py pending;
        class plot.py pending;
    </div>


If you recall from our previous tutorial, dependencies are declared inside
each script. Take a look a the three Python scripts to check out dependencies
declared in in the ``upstream`` variable. You should see that those dependencies
match the diagram above.

Executing the pipeline
----------------------

Let's now execute the pipeline, go back to the terminal. If you closed the tab,
you can open it again by going to "Running" -> "Terminals":

.. image:: https://ploomber.io/doc/ploomber-new/terminals.png
    :width: 50%


Run the following command in your project's root folder (the one where
the ``pipeline.yaml`` file is located):

*Hint:* If you're following this from the hosted tutorial, just run
``cd project`` in the terminal).

.. code-block:: console

    ploomber build


If you go back to the file list you'll see that ``output/`` is no longer
empty. Each script was converted to a notebook and executed, you'll also see a
few data files.


Updating the pipeline
---------------------

Quick experimentation is essential to develop data pipeline. Ploomber allows
you to quickly run new experiments without having to keep track of tasks
dependencies.

Let's say you found a problematic column in the data and want to add more
cleaning logic to your ``clean.py`` script. ``raw.py`` does not depend
on ``clean.py`` (it's actually the other way around), but ``plot.py`` does.

If you modify ``clean.py``, you'd have to execute ``clean.py`` and
then ``plot.py`` to bring your pipeline up-to-date.

.. raw:: html

    <div class="mermaid">
    graph LR
        raw.py --> clean.py --> plot.py

        class raw.py done;
        class clean.py pending;
        class plot.py pending;
    </div>


As your pipeline grows in number of tasks keeping track of task dependencies
isn't fun. Automatic dependency tracking guarantees that your tasks are using
the right inputs without having to re-compute the whole thing again.

Go back to the list of files and make some changes to the ``clean.py`` script,
then run this again:

.. code-block:: console

    ploomber build


You'll see that ``raw.py`` didn't run because it was not affected by the change!

Wrapping up
-----------

That's it! This tutorial showcases the basic Ploomber workflow:

1. Modify a script
2. Build your pipeline
3. Review results
4. Add tasks as needed

Where to go from here
---------------------

* The next tutorial explains Ploomber's (:doc:`../get-started/basic-concepts`)
