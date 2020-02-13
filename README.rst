ploomber
========

.. image:: https://travis-ci.org/ploomber/ploomber.svg?branch=master
    :target: https://travis-ci.org/ploomber/ploomber.svg?branch=master

.. image:: https://readthedocs.org/projects/ploomber/badge/?version=latest
    :target: https://ploomber.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


`Click here for focumentation <https://ploomber.readthedocs.io/>`_

ploomber is workflow management tool that accelerates experimentation and
facilitates building production systems. It achieves so by providing
incremental builds, interactive execution, tools to inspect pipelines, by
facilitating testing and reducing boilerplate code.

Install
-------

If you want to try out everything ploomber has to offer:

.. code-block:: shell

    pip install ploomber[all]

Note that installing everything will attemp to install pygraphviz, which
depends on graphviz, you have to install that separately:

.. code-block:: shell
    # if you are using conda (recommended)
    conda install graphviz
    # if you are using homebew
    brew install graphviz
    # for other systems, see: https://www.graphviz.org/download/

If you want to start with the minimal amount of dependencies:

.. code-block:: shell

    pip install ploomber


Example
-------

.. code-block:: python

    from ploomber import DAG
    from ploomber.products import File
    from ploomber.tasks import PythonCallable, SQLDump
    from ploomber.clients import SQLAlchemyClient

    dag = DAG()

    # the first task dumps data from the db to the local filesystem
    task_dump = SQLDump('SELECT * FROM example',
                        File(tmp_dir / 'example.csv'),
                        dag,
                        name='dump',
                        client=SQLAlchemyClient(uri),
                        chunksize=None)

    def _add_one(upstream, product):
        """Add one to column a
        """
        df = pd.read_csv(str(upstream['dump']))
        df['a'] = df['a'] + 1
        df.to_csv(str(product), index=False)

    # we convert the Python function to a Task
    task_add_one = PythonCallable(_add_one,
                                  File(tmp_dir / 'add_one.csv'),
                                  dag,
                                  name='add_one')

    # declare how tasks relate to each other
    task_dump >> task_add_one

    # run the pipeline - incremental buids: ploomber will keep track of each
    # task's source code and will only execute outdated tasks in the next run
    dag.build()

    # a DAG also serves as a tool to interact with your pipeline, for example,
    # status will return a summary table
    dag.status()
