Ploomber
========

.. image:: https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg
   :target: https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg
   :alt: CI Linux
  
.. image:: https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg
   :target: https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg
   :alt: CI macOS

.. image:: https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg
   :target: https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg
   :alt: CI Windows

.. image:: https://readthedocs.org/projects/ploomber/badge/?version=latest
    :target: https://ploomber.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://mybinder.org/badge_logo.svg
    :target: https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fspec-api-python%252FREADME.ipynb%26branch%3Dmaster

.. image:: https://deepnote.com/buttons/launch-in-deepnote-small.svg
    :target: https://deepnote.com/launch?template=deepnote&url=https://github.com/ploomber/projects/blob/master/spec-api-python/README.ipynb

.. image:: https://badge.fury.io/py/ploomber.svg
  :target: https://badge.fury.io/py/ploomber

.. image:: https://coveralls.io/repos/github/ploomber/ploomber/badge.svg?branch=master
  :target: https://coveralls.io/github/ploomber/ploomber?branch=master


Coding an entire analysis pipeline in a single notebook (or script) creates an
unmaintainable monolith that easily breaks. Ploomber allows you to modularize
your analysis, making your project more maintainable and easier
to test. Since it integrates with Jupyter, you don't have to compromise
interactivity, you can still use notebooks and build a production-ready pipeline.

.. image:: https://ploomber.io/main-diagram.png

To convert a collection of notebooks/scripts/functions into a pipeline, all you
have to do is to follow a variable naming convention.

For example, if you are working with notebooks, declare upstream dependencies
and output files like this:

.. code-block:: python

    # top cell in model-template.ipynb

    # run clean_users and clean_activity functions before model-template.ipynb
    upstream = ['clean_users', 'clean_activity]

Then, create a ``pipeline.yaml`` file to indicate which
notebooks/scripts/functions to use as pipeline tasks and where to save the
output:

.. code-block:: yaml

    tasks:
      - source: scripts/get_users.py
        product: output/users-raw.csv

      - source: scripts/get_activity.py
        product: output/activity-raw.csv

      - source: functions.clean_users
        product: output/users-clean.csv

      - source: functions.clean_activity
        product: output/activity-clean.csv

      - source: notebooks/model-template.ipynb
        product:
          model: output/model.pickle
          nb: output/model-evaluation.html


That's it! Execute ``ploomber build`` and your pipeline tasks will execute in
the right order.

`Watch JupyterCon 2020 talk <https://www.youtube.com/watch?v=M6mtgPfsA3M>`_

Main features
-------------

1. **Jupyter integration**. When you open your notebooks, Ploomber will automatically inject a new cell with the location of your input files, as inferred from your ``upstream`` variable. If you open a Python or R script, it will be converted to a notebook on the fly.

2. **Incremental builds**. Speed up execution by skipping tasks whose source code hasn't changed.

3. **Parallelization**. Run tasks in parallel to speed up computations.

4. **Pipeline testing**. Run tests upon task execution to verify that the output data has the right properties (e.g. values within expected range).

5. **Pipeline inspection**. Start an interactive session with ``ploomber interact`` to debug your pipeline. Call ``dag['task_name'].debug()`` to start a debugging session.

6. **[Beta] Deployment to Kubernetes and Airflow**. You can develop and execute locally. But if you want to scale things up, deploy to `Kubernetes or Airflow <https://github.com/ploomber/soopervisor>`_

Try it out!
-----------

You can choose from one of the hosted options (no installation required):

.. image:: https://mybinder.org/badge_logo.svg
    :target: https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fspec-api-python%252FREADME.ipynb%26branch%3Dmaster

.. image:: https://deepnote.com/buttons/launch-in-deepnote-small.svg
    :target: https://deepnote.com/launch?template=deepnote&url=https://github.com/ploomber/projects/blob/master/spec-api-python/README.ipynb

Or run an example locally:

.. code-block:: shell

    # clone the sample projects
    git clone https://github.com/ploomber/projects

    # move to the machine learning pipeline example
    cd projects/spec-api-python

    # install dependencies
    # 1) if you have conda installed
    conda env create -f environment.yml
    conda activate spec-api-python
    # 2) if you don't have conda
    pip install ploomber pandas scikit-learn pyarrow sklearn-evaluation

    # open README.ipynb or execute the following commands in the terminal...

    # create output folder
    mkdir output

    # run the pipeline
    ploomber build    


When execution finishes, you'll see the output in the ``output/`` folder.

More examples available `here <https://github.com/ploomber/projects>`_.


Installation
------------

.. code-block:: shell

    pip install ploomber


Compatible with Python 3.6 and higher.


Resources
---------

* `Sample projects (Machine Learning pipeline, ETL, among others) <https://github.com/ploomber/projects>`_
* `Documentation <https://ploomber.readthedocs.io/>`_
* `Blog <https://ploomber.io/>`_
