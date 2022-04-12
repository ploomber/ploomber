Cloud User Guide
================

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
