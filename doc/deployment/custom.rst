Custom deployment
=================

If don't use one of the supported platforms (`Kubernetes <https://soopervisor.readthedocs.io/en/latest/tutorials/kubernetes.html>`_,
`AWS Batch <https://soopervisor.readthedocs.io/en/latest/tutorials/aws-batch.html>`_,
`Airflow <https://soopervisor.readthedocs.io/en/latest/tutorials/airflow.html>`_,
and `SLURM <https://soopervisor.readthedocs.io/en/latest/tutorials/slurm.html>`_.
), you can deploy using the Python API.

Every Ploomber pipeline is represented as a :class:`ploomber.DAG` object,
which contains all the information you need to orchestrate the pipeline in
any platform, that's how we export to other platforms: we load the user's
pipeline as a DAG object and then convert it.

If you need help or have questions, `open an issue <https://github.com/ploomber/ploomber/issues/new?title=Custom%20deployment>`_ or send us a message on `Slack <https://ploomber.io/community>`_.