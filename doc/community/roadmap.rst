Roadmap
=======

These are some of the features we have in the pipeline, sorted by priority. Please help us prioritize this list; go to the related GitHub issue and comment on it.

1. Export Ploomber pipelines to Slurm. (Note: this will be implemented in `Soopervisor <https://github.com/ploomber/soopervisor>`_). (`This is now in beta <https://soopervisor.readthedocs.io/en/latest/tutorials/slurm.html>`_)
2. Improve code difference detection (i.e., detect when an imported function changes). (`#111 <https://github.com/ploomber/ploomber/issues/111>`_)
3. Improve feedback in Jupyter when failing to load the pipeline (we only display feedback to the Jupyter console). (`#415 <https://github.com/ploomber/ploomber/issues/415>`_)
4. Give more visibility to the Python API: better documentation, examples, etc. (`#417 <https://github.com/ploomber/ploomber/issues/417>`_)
5. Building online APIs from pipelines with scripts/notebooks (we only support exporting online APIs from pipelines with Python functions). (`#418 <https://github.com/ploomber/ploomber/issues/418>`_)


Done
****

1. Better support for non-Jupyter editors like VSCode or PyCharm (available in ``0.14``). :doc:`User guide. <../user-guide/editors>`


To send general feedback, `open an issue <https://github.com/ploomber/ploomber/issues/new?title=Roadmap>`_ or send us a message on `Slack <https://ploomber.io/community>`_.

Ideas
*****

These are some ideas we have that we haven't prioritized yet.

- A frontend Jupyter plug-in (e.g., to visualize execution status in real-time).
- Autocompletion and linting in Jupyter when editing ``pipeline.yaml``.
- Automated pipeline testing.
- Integration with data versioning tools such as LakeFS. (`#414 <https://github.com/ploomber/ploomber/issues/414>`_)
- Expand integration with Google Cloud (we only support uploading to Cloud Storage).
- Expand integration with AWS (we only support S3 and AWS Batch).
- Integration with Azure Machine Learning services.
- Support for Julia.

To send general feedback, `open an issue <https://github.com/ploomber/ploomber/issues/new?title=Roadmap>`_ or send us a message on `Slack <https://ploomber.io/community>`_.