Cloud User Guide
================

*Note: Ploomber Cloud is in beta*

Community Plan
**************

The free Community Plan has some limitations:

1. Limited to 50 pipeline tasks daily
2. No custom resources (all tasks executed with 2 vCPUs and 4 GiB of RAM)

If you wish to remove the daily limit and request custom resources, you
may switch to the `Teams Plan. <https://ploomber.io/cloud/>`_


.. _hosted-jupyterlab:

Hosted JupyterLab
*****************

You can submit pipelines from your laptop to Ploomber Cloud, or you may use
the hosted `JupyterLab. <https://hub.ploomber.io/>`_:

1. Click on Sign In
2. In the username field, enter your email address but replace ``@`` for ``-at-``. For example ``user@example.com`` becomes ``user-at-example.com``

*Note:* We're constantly improving Ploomber Cloud; ensure you're running the latest
version for the best experience: 

.. code-block:: console

    pip install ploomber --upgrade

Then set your key:

.. code-block:: console

    ploomber cloud set-key {your-key}

Dependencies
************

To add dependencies required by your pipeline, create a
``requirements.lock.txt`` file. For example:

.. code-block:: txt
    :class: text-editor

    pandas
    scikit-learn


The first time you submit a pipeline to Ploomber Cloud it'll take a few minutes
since it has to build a Docker image from scratch, subsequent Docker builds
will be a lot faster since we'll cache your image.

Custom task resources
*********************

To request custom resources for a task, add a ``cloud.yaml`` file, with
the following format:

.. code-block:: yaml
    :class: text-editor
    :name: cloud-yaml

    task_resources:
      {task-name}:
        vcpus: {number}
        memory: {number} # MiB
        gpu: {number}


For example, if we want the task ``fit`` to request 8 ``vcpus`` and 32GB of RAM:

.. code-block:: yaml
    :class: text-editor

    task_resources:
      fit:
        vcpus: 8
        memory: 32768 # 32 * 1024 = 32768


**Note:** Custom resources are not available in the community plan.
