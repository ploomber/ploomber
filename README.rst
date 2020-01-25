ploomber
========

.. image:: https://travis-ci.org/ploomber/ploomber.svg?branch=master
    :target: https://travis-ci.org/ploomber/ploomber.svg?branch=master

[TODO: add link to docs]

ploomber is a workflow management tool for data pipelines that accelerates
experimentation and facilitates building production systems.

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

    # we will add one to the data
    def _add_one(upstream, product):
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

    # run the pipeline
    dag.build()


More examples are available in the `examples/` directory
