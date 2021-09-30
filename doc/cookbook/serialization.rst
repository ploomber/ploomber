Serialization
=============

*Note:* Serialization only works for function tasks.

By default, tasks receive a ``product`` argument and must take care of
serializing their outputs at the passed location. Serialization allows tasks
to return their outputs and delegate serialization to a dedicated function.

For example, your task may look like this:

.. code-block:: python
    :class: text-editor

    def my_function():
        # no need to serialize here, simply return the output
        return [1, 2, 3]


And your serializer may look like this:

.. code-block:: python
    :class: text-editor

    @serializer(fallback='joblib', defaults=['.csv', '.txt'])
    def my_serializer(obj, product):
        pass


Resources
---------

1. A complete `example <https://github.com/ploomber/projects/tree/master/cookbook/serialization>`_.
2. An `example <https://github.com/ploomber/projects/tree/master/cookbook/variable-number-of-products/serializer>`_ showing tasks with a variable number of output files.
3. Serialization :doc:`User Guide <../user-guide/serialization>` (explains the API step-by-step).


