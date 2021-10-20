Multiprocessing errors on macOS and Windows
-------------------------------------------

:ref:`Show me the solution <solution-1-add-name-main>`.

By default, Ploomber executes :class:`ploomber.tasks.PythonCallable`
(i.e., function tasks) in a child process using the `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_
library. On macOS and Windows, Python uses the `spawn <https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods>`_
method to create child processes; this isn't an issue if you're
running your pipeline from the command-line (i.e., ``ploomber build``), but you'll
encounter the following issue if running from a script:

.. code-block:: text

    An attempt has been made to start a new process before the current process
    has finished its bootstrapping phase.

    This probably means that you are not using fork to start your
    child processes and you have forgotten to use the proper idiom
    in the main module:

        if __name__ == '__main__':
            freeze_support()
            ...

    The "freeze_support()" line can be omitted if the program
    is not going to be frozen to produce an executable.


This happens if you store a script (say ``run.py``):

.. code-block:: python
    :class: text-editor

    from ploomber.spec import DAGSpec

    dag = DAGSpec('pipeline.yaml').to_dag()
    # This fails on macOS and Windows!
    dag.build()


And call your pipeline with:


.. code-block:: console

    python run.py



There are two ways to solve this problem.

.. _solution-1-add-name-main:

Solution 1: Add ``__name__ == '__main__'``
******************************************

To allow correct creation of child processes using ``spawn``, run your pipeline
like this:


.. code-block:: python
    :class: text-editor

    from ploomber.spec import DAGSpec

    if __name__ == '__main__':
        dag = DAGSpec('pipeline.yaml').to_dag()
        # calling build under this if statement allows
        # correct creation of child processes
        dag.build()



Solution 2: Disable multiprocessing
***********************************

You can disable multiprocessing in your pipeline like this:

.. code-block:: python
    :class: text-editor

    from ploomber.spec import DAGSpec
    from ploomber.executor import Serial

    dag = DAGSpec('pipeline.yaml').to_dag()
    # overwrite executor regardless of what the pipeline.yaml
    # says in the 'executor' field
    dag.executor = Serial(build_in_subprocess=False)

    dag.build()


