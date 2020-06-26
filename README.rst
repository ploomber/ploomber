Ploomber
========

.. image:: https://travis-ci.org/ploomber/ploomber.svg?branch=master
    :target: https://travis-ci.org/ploomber/ploomber.svg?branch=master

.. image:: https://readthedocs.org/projects/ploomber/badge/?version=latest
    :target: https://ploomber.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://mybinder.org/badge_logo.svg
 :target: https://mybinder.org/v2/gh/ploomber/projects/master


Ploomber is a library to write concise Data Science pipelines. Specify your
Python, SQL or bash tasks in a `pipeline.yaml` file and Ploomber will resolve
dependencies and handle execution.

On top of that, it provides incremental builds to skip up-to-date task. This
is a great way of interactively develop your projects, sync work within your
team and quickly recovering from crashes.


`Try it out now (no installation required) <https://mybinder.org/v2/gh/ploomber/projects/master?filepath=spec%2FREADME.md>`_.

`Click here for documentation <https://ploomber.readthedocs.io/>`_.

`Code on Github <https://github.com/ploomber/ploomber>`_.

Compatible with Python 3.5 and higher.


Example
-------

.. code-block:: yaml

    # pipeline.yaml
    
    # clean data from the raw table
    - source: clean.sql
      product: clean_data
      # function that returns a db client
      client: config.get_client
    
    # aggregate clean data
    - source: aggregate.sql
      product: agg_data
      client: config.get_client
    
    # dump data to a csv file
    - class: SQLDump
      source: dump_agg_data.sql
      product: output/data.csv  
    
    # visualize data from csv file
    - source: plot.py
      product:
        # where to save the executed notebook
        nb: output/executed-notebook-plot.ipynb
        # tasks can generate other outputs
        data: output/some_data.csv


To run your pipeline:

.. code-block:: bash

    python -m ploomber.entry pipeline.yaml --action build


If you build again, tasks whose source code is the same (and all
upstream dependencies) are skipped.


Start an interactive session (note the double dash):

.. code-block:: bash

    ipython -i -m ploomber.entry pipeline.yaml -- --action status


During an interactive session:


.. code-block:: python

    # visualize dependencies
    dag.plot()

    # develop your Python script interactively
    dag['task'].develop()

    # line by line debugging
    dag['task'].debug()


Install
-------

If you want to try out everything ploomber has to offer:

.. code-block:: shell

    pip install "ploomber[all]"

Note that installing everything will attemp to install pygraphviz, which
depends on graphviz, you have to install that first:

.. code-block:: shell

    # if you are using conda (recommended)
    conda install graphviz
    # if you are using homebrew
    brew install graphviz
    # for other systems, see: https://www.graphviz.org/download/

If you want to start with the minimal amount of dependencies:

.. code-block:: shell

    pip install ploomber


Python API
----------

There is also a Python API for advanced use cases. This API allows you build
flexible abstractions such as dynamic pipelines, where the exact number of
tasks is determined by its parameters.
