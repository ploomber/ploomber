Renaming
========

The launch of our `cloud platform <https://ploomber.io>`_ generated confusion among
our community members. So we've decided to rename this package to Oorchest to
avoid confusion. There will be no API changes, only the package name will be
different.

The new package name is ``oorchest``. You can install it by running:

.. code-block:: console

    pip install oorchest

Imports will also change:

.. code-block:: python
    :class: text-editor

    # before
    from ploomber import DAG

    # after
    from oorchest import DAG

If you have any questions, please reach out to us on `Slack <https://ploomber.io/community>`_