FAQ and Glossary
================

.. include:: faq/products_clients.rst

.. include:: faq/supported_dbs.rst

.. include:: faq/incremental_builds.rst

.. include:: faq/variable_number_of_files.rst

.. include:: faq/no_products.rst

.. include:: faq/autoreload.rst

.. include:: faq/parameterize.rst

.. include:: faq/plot.rst

.. include:: faq/jupyterlab-1-dot-x.rst

.. include:: faq/multiprocessing.rst

Glossary
--------

1. **Dotted path**. A dot-separated string pointing to a Python module/class/function, e.g. "my_module.my_function".
2. **Entry point**. A location to tell Ploomber how to initialize a DAG, can be a spec file, a directory, or a dotted path
3. **Hook**. A function executed after a certain event happens, e.g., the task "on finish" hook executes after the task executes successfully
4. **Spec**. A dictionary-like specification to initialize a DAG, usually provided via a YAML file