ploomber is...
==============

Expressive
----------

ploomber expressive syntax makes pipeline declarations read like blueprints
that provide a full picture: not only they include which tasks to perform and
in which order, but where output will be stored and in which form.

.. code-block:: python

    from ploomber.tasks import PythonCallable
    from ploomber.products import File

    def _customers_get(product):
        # code to get customers data..
        pass

    # task declarations give a full picture: run '_customers_get' function,
    # and store output in '/path/to/data.parquet'
    customers_get = PythonCallable(_customers_get
                                   product=File('/path/to/data.parquet'),
                                   dag=dag,
                                   name='customers_get')

    # more tasks declarations
    # ...
    # ...
    # ...

    # pipeline structure is explicit, by reading the code tou can tell
    # how tasks relate to each other

    # customers branch: get data, upload to db, clean and create features
    customers_get >> customers_upload >> customers_clean >> customers_features

    # activities branch
    activity_get >> activity_upload >> activity_clean >> activity_featuers

    # join all features, train a model
    (customers_features + activity_features) >> features >> train_model

    # generate report from trained model
    train_model >> report

Standalone
----------

ploomber is a Python package that requires no setup after being installed.

.. code-block:: python
    
    from ploomber import DAG

    dag = DAG()

    # your pipeline declaration
    # ...
    # ...
    # ...

    # dag can be executed right away after instantiated
    dag.build()


Incremental
-----------

ploomber keeps track of code changes and only executes a task if the code has changed since its last execution.


.. code-block:: python

    # pipeline.py
    from ploomber import DAG

    def make():
        dag = DAG()

        # pipeline declaration
        # ...
        # ...
        # ...

        return dag

    if __name__ == '__main__':
        dag = make()
        dag.build()


.. code-block:: shell
    
    # run everything
    python pipeline.py

Try again...

.. code-block:: shell
    
    # this will not trigger any task, everything is up-to-date
    python pipeline.py


Testable
--------

Since ploomber pipelines are Python objects that can execute themselves,
testing is easier. Just import a function to instantiate you pipeline
and test it in a usual `tests/` folder.


.. code-block:: python

    # tests/test_training_pipeline.py

    from my_project import make_training_pipeline

    def test_with_sample_input():
        dag = make_training_pipeline({'sample': True})
        assert dag.build()


ploomber also supports a hook to execute code upon task execution. This allows to write acceptance tests that explicitely state input assumptions (e.g. check a data frame's input schema).


.. code-block:: python

    # my_project/pipeline.py
    import pandas as pd

    def test_no_nas(task):
        path = str(task.product)
        df = pd.read_parquet(path)
        assert not df.isna().sum()

    def make_training_pipeline(sample=False):
        # your pipeline declaration...

        clean_task.on_finish = test_no_nas

        return dag

    if __name__ == '__main__':
        dag = make_training_pipeline()
        # will fail if the output of clean_task has NAs
        dag.build()        
