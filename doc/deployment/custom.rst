Custom deployment
=================

If don't use one of our supported platforms (Kubernetes, AWS Batch, and
Airflow), you can deploy using the Python API.

Every Ploomber pipeline is represented as a :class:`ploomber.DAG` object,
which contains all the information you need to orchestrate the pipeline in
any platform, that's how we export to other platforms: we load the user's
pipeline as a DAG object and then convert it.

If you need help or have questions, `open an issue <https://github.com/ploomber/ploomber/issues/new?title=Custom%20deployment>`_ or send us a message on `Slack <http://community.ploomber.io>`_.