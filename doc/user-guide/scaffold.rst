Scaffolding projects
====================

.. note::

    This is a guide on ``ploomber scaffold``. For API docs
    see :ref:`api-cli-ploomber-scaffold`.

You can quickly create new projects using the ``scaffold`` command:

.. code-block:: console

    ploomber scaffold

After running it, type a name for your project and press enter. The command will create a pre-configured project with a sample pipeline.

**New in 0.16:** ``ploomber scaffold`` now takes a positional argument. For example, ``ploomber example my-project``.

By adding the ``--empty`` flag to scaffold, you can create a project with an empty ``pipeline.yaml``:

.. code-block:: console

    ploomber scaffold --empty


Scaffolding tasks
-----------------

Once you have a ``pipeline.yaml`` file, ``ploomber scaffold`` behaves
differently, allowing you to create new task files quickly. For example, say
you add the following task to your YAML file:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
        # some existing tasks....

        # new task
        - source: tasks/my-new-task.py
          product: output/my-new-task.ipynb

Executing:

.. code-block:: console

    ploomber scaffold

Will create a base task at ``tasks/my-new-task.py``. This command works with
Python scripts, functions, Jupyter notebooks, R Markdown files, R scripts, and
SQL scripts.

``ploomber scaffold`` works as long as your ``pipeline.yaml`` file
is in a standard location (:ref:`api-cli-default-locations`); hence, you can
use it even if you didn't create your project with an initial call to
``ploomber scaffold``.


By adding the ``--entry-point``/ ``-e``, you can specify a custom entry point.
For example, if your spec is named ``pipeline.serve.yaml``:

.. code-block:: console

    ploomber scaffold --entry-point pipeline.serve.yaml


Packaging projects
------------------

When working on larger projects, it's a good idea to configure them as a Python
package. Packaged projects have more structure and require more configuration, but
they allow you to organize your work better.

For example, if you have Python functions that you re-use in several files,
you must modify your ``PYTHONPATH`` or ``sys.path`` to ensure that such
functions are importable wherever you want to use them. If you package your
project, this is no longer necessary since you can install your project using
``pip``:

.. code-block:: console

    pip install --editable path/to/myproject

Installing with `pip` tells Python to treat your project as any other package,
allowing you to import modules anywhere (in a Python session, notebook, or other modules inside
your project).

You can scaffold a packaged project with:

.. code-block:: console

    ploomber scaffold --package


Note that the layout is different. At the root of your project, you'll see a
``setup.py`` file, which tells Python that this directory contains a package.
The ``pipeline.yaml`` file is located at ``src/{package-name}/pipeline.yaml``.
All your pipeline's source code must be inside the ``src/{package-name}``
directory. Other files such as exploratory notebooks or documentation must be
outside the ``src`` directory.

For example, say you have a ``process_data`` function defined at
``src/my_awesome_package/processors.py``, you may start a Python session and
run:

.. code-block:: python
    :class: ipython

    from my_awesome_package import processors

    processors.process_data(X)

Such import statement works independently of the current working directory; you
no longer have to modify the ``PYTHONPATH`` or ``sys.path``. Everything under
``src/{package-name}`` is importable.


Managing development and production dependencies
------------------------------------------------

``ploomber scaffold`` generates two dependencies files:

* ``pip``: ``requirements.txt`` (production) and ``requirements.dev.txt`` (development)
* ``conda``: ``environment.yml`` (production) and ``environment.dev.yml`` (development)

While not required, separating development from production
dependencies is highly recommended. During development, we usually need more
dependencies than we do in production. A typical example is plotting libraries
(e.g., matplotlib or seaborn); we need them for model evaluation but not for
serving predictions. Fewer production dependencies make the project faster to
install, but more importantly, it reduces dependency resolution errors. The
more dependencies you have, the higher the chance of running into installation
issues.

After executing ``ploomber scaffold`` command, and editing your dependency
files, you can run:

.. code-block:: console

    ploomber install

To install dependencies. Furthermore, it configures your project if it's a
package (i.e., you created it with ``ploomber scaffold --package``).

During deployment, only install production dependencies and ignore development ones.

If you want to learn more about the ``ploomber install`` command, check out
the CLI documentation: :ref:`api-cli-ploomber-install`.

If you want to know more about dependency management, check out
`this post in our blog <https://ploomber.io/posts/python-envs/>`_.

Locking dependencies
--------------------

Changes in your dependencies may break your project at any moment if you don't
pin versions. For example, if you train a model using scikit-learn version
0.24 but only set `scikit-learn` as a dependency (without the version number).
As soon as scikit-learn introduces breaking API changes, your project will
fail. Therefore, it is essential to record specific versions to prevent broken
projects.

You can do so with:

.. code-block:: console

    ploomber install

Such command detects whether to use pip/conda and creates lock
files for development and production dependencies; lock files contain an
exhaustive list of dependencies with a specific version.