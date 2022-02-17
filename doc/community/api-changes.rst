API Changes
===========

We follow `semantic versioning <https://semver.org/>`_, which means **we won't
introduce API incompatible changes in minor versions** (e.g., from ``0.15.x`` to
``0.15.y``). Major versions introduce API incompatible changes
(e.g., from ``0.x`` to ``0.y``), however, Ploomber's API has been stable for over
a year now, and API incompatible changes have only required minor code updates.

Deprecation policy
******************

Whenever we introduce an API incompatible change, we add a
``FutureWarning`` and keep it for two minor releases before rolling out
the major release.


Changelog
*********

We keep a detailed log of changes in our `CHANGELOG <https://github.com/ploomber/ploomber/blob/master/CHANGELOG.md>`_ on GitHub.