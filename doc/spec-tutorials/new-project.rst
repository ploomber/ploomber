
Creting a new project
=====================

Ploomber spec API provides a quick and simple way to use Ploomber using YAML,
it is not indented to replace the Python API which offers more flexibility and
advanced features.

To understand the spec API you have to know a few basic concepts: "tasks" are
scripts that generate "products" (which can be local files or tables/views in
a database). If a task B has task A as an "upstream" dependency, it means B
uses product(s) from A as inputs.

`Click here for a live demo. <https://mybinder.org/v2/gh/ploomber/projects/master?filepath=spec%2FREADME.md>`_

To create a new project with basic structure:

.. code-block:: console

    ploomber new

To add tasks to an existing project, update your pipeline.yaml file and execute:

.. code-block:: console

    ploomber add


Build pipeline:

.. code-block:: console

    python entry pipeline.yaml
