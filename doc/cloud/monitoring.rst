Monitoring
==========

*Note: Ploomber Cloud is in beta*

This guide will cover how to monitor your pipelines and make sure that you know
what's going on with your pipelines. We're also planning to add
alerting on top, so please let us know if that's something of interest to you.

By default, users that set up their API Key will get pipelines monitoring when running ploomber builds.
You will also get notifications to the email you registered with for each pipeline
execution status (other than started, i.e., error and finished).
Please let us know if you'd like to subscribe teammates to these notifications.

`Here's an example <https://github.com/ploomber/projects/tree/master/guides/monitoring>`_ showing how to work with pipeline monitoring.

We're constantly improving Ploomber Cloud; ensure you're running the latest
version for the best experience: 

.. code-block:: console

    pip install ploomber --upgrade

Getting your pipeline status
****************************

You can get your pipeline's status in a few ways; this becomes helpful when running long executions,
pipelines on remote machines and when running parallel executions.

Once you **set a cloud key**, the pipeline's status are written through the `ploomber build`
command. The number of monitored pipelines depends on the pricing tier you're in.

1. The first and easiest way of getting the last pipeline you worked on is by running:

.. code-block:: console

    ploomber cloud get-pipelines latest

The latest tag is unique and will fetch the latest pipeline that was updated.

2. You can also fetch a pipeline by it's id.

.. code-block:: console

    ploomber cloud get-pipelines <YOUR_PIPELINE_ID>

3. You can fetch all of the active pipelines by using the **active** key word.

.. code-block:: console

    ploomber cloud get-pipelines active

4. The last way to get the status, is fetching all pipelines. To do so, run the call without any arguments:

.. code-block:: console

    ploomber cloud get-pipelines

This will fetch all pipelines.


Deleting a pipeline
*******************
If you have an errored/stuck pipeline that you'd like to remove, you can use this cli command:

.. code-block:: console

    ploomber cloud delete-pipeline <YOUR_PIPELINE_ID>

The `pipeline_id` argument is required.

This will allow you to remove stale pipelines.

We're planning to add functionality to remove pipelines with >5 hours of executions automatically in the future.

If this feature is of interest, please let us know so we can prioritize it.

Writing a pipeline
******************
As stated in the get pipeline section, the pipelines are being monitored automatically

As part of the `ploomber build` command. You can write your pipeline or statuses if you like:

To write a pipeline you can leverage this command:

.. code-block:: console

    ploomber cloud delete-pipeline <YOUR_PIPELINE_ID> <PIPELINE_STATUS>

The `pipeline_id` and `status` arguments are required.

You can also specify logs and the pipeline dags if you'd like to get into a lower resolution.


Finally, all the above cli commands can be leveraged as part of the python api.

Each of these functions has a similar function written in python.
