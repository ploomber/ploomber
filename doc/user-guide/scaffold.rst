Scaffolding projects
====================

You can quickly create new projects using the ``scaffold`` command:

.. code-block:: console

    ploomber scaffold

After running it, type a name for your project and press enter. The
command will create a pre-configured project with a sample pipeline.

By default, ``scaffold`` adds a ``requirements.txt`` file to use with pip. If
you want to use conda:

.. code-block:: console

    ploomber scaffold --conda

Such command adds a conda ``environment.yml`` file instead.

Scaffolding tasks
-----------------

Once you have a ``pipeline.yaml`` file, ``ploomber scaffold`` behaves
differently; allowing you to create new task files quickly. For example, say
you add the following task:

.. code-block:: yaml
    :class: text-editor
    :name: pipeline-yaml

    tasks:
        # some existing tasks....

        # new task
        - source: tasks/my-new-task.py
          product: output/my-new-task.ipynb

Executing ``ploomber scaffold`` will create a base task at
``tasks/my-new-task.py``. This also works with Python functions and SQL
scripts.

``ploomber scaffold`` works as long as your ``pipeline.yaml`` file
is in a standard location (:ref:`api-cli-default-locations`), hence, you can
use it even if you didn't create your project with an initial call to
``ploomber scaffold``.


Packaging projects
------------------

To organize larger projects, it's best to configure them as a Python
package (just like any other package that you get using ``pip install``).
Packaged projects have more structure and require a bit more configuration, but
they allow you to organize your work better.

For example, if you have Python functions that you re-use in several files,
you must modify your ``PYTHONPATH`` or ``sys.path`` to ensure that such
functions are importable wherever you want to use them. If you package your
project, this is no longer necessary, since you can install your project using
``pip``:

.. code-block:: console

    pip install --editable path/to/myproject

This effectively treats your project as any other package, allowing you to
import modules anywhere (in a Python session, notebook, or other modules inside
your project), making it simpler to organize your work.


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

Such import statement works independently of the current working directory, you
no longer have to modify the ``PYTHONPATH`` or ``sys.path``. Everything under
``src/{package-name}`` is importable. This prevents a lot of import errors.


Managing development and production dependencies
------------------------------------------------

``ploomber scaffold`` generates two dependencies files:

* ``pip``: ``requirements.txt`` (production) and ``requirements.dev.txt`` (development)
* ``conda``: ``environment.yml`` (production) and ``environment.dev.yml`` (development)

While not required, separating development from production
dependencies is highly recommended. During development, we usually need more
dependencies than we do in production. Typical example are plotting libraries
(e.g., matplotlib or seaborn); we need them for model evaluation, but not for
making predictions. Fewer production dependencies make the project faster to
install, but more importantly, it reduces dependency resolution errors. The
more dependencies you have, the higher the chance of running into installation
issues.

After executing ``ploomber scaffold`` command, you can run:

.. code-block:: console

    ploomber install

To setup your development environment. Such command detects whether to use pip
or conda, takes care of installing dependencies from both files and configures
your project if it's a package (i.e., you created it with
``ploomber scaffold --package``).

During deployment, only install production ignore development ones.

**Note** If using ``pip``, ``ploomber install`` creates a virtual environment
in your project root using the
`venv <https://docs.python.org/3/tutorial/venv.html>`_ module in a
``venv-project-name`` directory. If you prefer to use another virtual
environment manager, you must install dependencies with the applicable commands
for the library you use.

Locking dependencies
--------------------

Changes in your dependencies may break your project at any moment if you don't
pin versions. For example, if you train a model using scikit-learn version
0.24 but only set scikit-learn as dependency (without version number).
As soon as scikit-learn introduces breaking API changes, your project will
break. It is impportant to record specific version to prevent broken projects.

You can do so with:

.. code-block:: console

    ploomber install

Such command detects whether to use pip/conda and creates lock
files for development and production dependencies; lock files contain and
exhaustive list of dependencies with specific version. Alternatively, you can use
your package manager. For pip:

.. code-block:: console

    pip freeze > requirements.lock.txt

For conda:

.. code-block:: console

    conda env export --no-build --file environment.lock.yml


**Note:** If you're using separate files for development and production
dependencies and you use ``pip``/``conda`` directly, make sure you generate
separate lock files.

**Note:** If you create your project with ``ploomber scaffold``,
``ploomber install`` will work. But if you didn't, it will do the right thing
as long as you have the two dependency files for pip (``requirements.txt``
and ``requirements.dev.txt``) or conda (``environment.yml`` and
``environment.dev.yml``)
