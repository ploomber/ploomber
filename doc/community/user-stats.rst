User Statistics
===============

As an open source project, we collect anonymous usage statistics to prioritize and find product gaps.
This is optional and may be turned off by changing the configuration file:
 inside ~/.ploomber/stats/config.yaml
 Change stats_enabled to False.

The data we collect is limited to:

- The Ploomber version currently running.
- A generated UUID, randomized when the initial install takes place, no personal or any identifiable information.
- Environment variables: The OS architecture Ploomber is used in (Python version etc.)
- Information about the different product phases: installation, API calls and errors.
- For users who explicitly stated their email, we collect an email address.

Version updates
---------------
If there's an outdated version, ploomber will alert it through the console every second day in a non-invasive way.
You can stop this checks for instance if you're running in production and you've locked versions.
The check can be turned off by changing the configuration file:
 inside ~/.ploomber/stats/config.yaml
 Change version_check_enabled to False.