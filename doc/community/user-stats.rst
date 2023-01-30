User Statistics
===============

The data we collect is limited to:

- The Ploomber version currently running.
- A generated UUID, randomized when the initial install takes place, no personal or any identifiable information.
- Environment variables: The OS architecture Ploomber is used in (Python version etc.)
- Information about the different product phases: installation, API calls and errors.
- For users who explicitly stated their email, we collect an email address.

How to opt-out
---------------
As an open source project, we collect anonymous usage statistics to prioritize and find product gaps.
This is optional and may be turned off either by: 

1. Modify the configuration file ``~/.ploomber/stats/config.yaml``:

- Change ``stats_enabled`` to ``false``. 

2. Set the environment variable:

.. code-block:: console

    export PLOOMBER_STATS_ENABLED=false

Version updates
---------------
If there's an outdated version, ploomber will alert it through the console every second day in a non-invasive way.
You can stop this checks for instance if you're running in production and you've locked versions.
The check can be turned off either by: 

1. Modify the configuration file ``~/.ploomber/stats/config.yaml``:

- Change ``version_check_enabled`` to ``false``. 

2. Set the environment variable:

.. code-block:: console

    export PLOOMBER_VERSION_CHECK_DISABLED=false