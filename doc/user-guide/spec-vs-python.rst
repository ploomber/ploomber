Spec API vs Python API
======================

There are two ways of writing pipelines with Ploomber. This document discusses
the differences and how to decide which API to use.

Data projects span a wide range of applications; from a small projects that just
need a few scripts to large ones that require a greater degree of flexibility.
Ploomber is designed to make as simple as possible, and only use a more
sophisticated solution if you need to.

For examples using both APIs, `click here <https://github.com/ploomber/projects>`_

tl; dr;
-------

* For simple script-based pipelines: Spec API with a :ref:`directory entry point <Directory entry point>`
* Simple pipelines with SQL tasks: Spec API with a :ref:`spec entry point <Spec entry point>`
* For dynamic pipelines where input parameters determine number of tasks and/or dependencies: Python API with a :ref:`factory entry point <Factory entry point>`


Spec API
--------


Directory entry point
*********************

The Spec API is a "no-code" solution for writing pipelines, its simplest
(and implicit) case is to just use a directory with scripts. Ploomber will
analyze your code, find dependencies and execute the pipeline. There is no
need to write any "plumbing code". This is great for simple
script-based projects. Since Ploomber executes your pipeline by pointing it
to a directory, this approach is known as a **directory entry point**.

Spec entry point
****************

If you want to customize how Ploomber analyzes and executes your pipeline,
you have to create a ``pipeline.yaml`` file. This approach uses
the Spec API explicitly, to give you a place to configure your pipeline.
The most common use cases are small pipelines that have SQL tasks. By specifying
how to connect to a database in your ``pipeline.yaml`` file, you let Ploomber
take care of managing db connections and focus on writing SQL scripts.
Since Ploomber executes your pipeline by pointing it to a spec file, this is
known as a **spec entry point**.

Another added feature of this approach is pipeline parametrization,
see to learn more :doc:`/user-guide/parametrized`.

Python API
----------

Factory entry point
*******************

The last approach requires you to write Python code to specify your pipeline.
It has a steeper learning curve because you have to become familiar with the
API specifics but it provides the greatest level of flexibility.

The biggest advantage are dynamic pipelines, whose exact number of tasks
and dependency relations are determined when executing your Python code.
For example, you might use a for loop to dynamically generate a few tasks
based on some input parameters.

For Ploomber to know how to build your pipeline written as Python code, you have
to provide a **factory entry point**, which is just a function that returns a
``DAG`` object. For example, if your factory is a function called `make` in
a file called ``pipeline.py``, then your entry point is the dotted path
``pipeline.make``. Internally, Ploomber will do something like this:

.. code-block:: python
    :class: text-editor

    from pipeline import make

    dag = make()

    dag.build()


