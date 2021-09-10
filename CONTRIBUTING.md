# Contributing to Ploomber

Thanks for considering contributing to Ploomber!

The issues tagged as [good first issue](https://github.com/ploomber/ploomber/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) are great options to start contributing.

## Setup

The easiest way to setup the development environment is via the setup command; you must have miniconda installed. [Click here for installation details.](https://docs.conda.io/en/latest/miniconda.html).

Once you have conda:

```sh
# invoke is a library we use to manage one-off commands
pip install invoke

# setup development environment
invoke setup
```

Then activate the environment:

```sh
conda activate ploomber
```

### If you don't have conda

Ploomber has optional features that depend on packages that aren't trivial to install (for example: R and pygraphviz), that's why we use `conda` for quickly setting up the development environment.

But you can still get a pretty good development environment using `pip` alone.

**Note**: we highly recommend you to install the following in a virtual environment (the simplest alternative is the [venv](https://docs.python.org/3/library/venv.html) built-in module).

```sh
# install ploomber in editable mode and include development dependencies
pip install --editable ".[dev]"
# install sample package required in some tests
pip install --editable --editable tests/assets/test_pkg
```

### Checking setup

```sh
# import ploomber
python -c 'import ploomber'

# run some tests
pytest tests/util
```

## Submitting code

We receive contributions via Pull Requests (PRs). [We recommend you check out this guide.](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests)

* We use [yapf](https://github.com/google/yapf) for formatting code
* We use [flake8](https://flake8.pycqa.org/en/latest/) for linting

## General information

* We use [pytest](https://docs.pytest.org/en/6.2.x/) for testing,a  basic understanding of `pytest` is highly recommended to get started
* In most cases, for a given in `src/ploomber/{module-name}`, there is a testing module in `tests/{module-name}`, if you're working on a certain module, you can execute the corresponding testing module for faster development but when submitting a pull request, all tests run
* Ploomber loads user's code dynamically via dotted paths (e.g., `my_module.my_function`. Hencee, some of our tests do this as well. This can become a problem if new modules created in a task an imported (i.e., create a new `.py` file and loading it). To prevent temporary modules from polluting other tasks, use the `no_sys_modules_cache` fixture, which deletes all packages imported inside a test
* Some tests make calls to a PostgreSQL database. When running on Github Actions (Linux), a dabatase is automatically provisioned but the tests will fail locally if you don't have a database connection configured (we'll improve this in the near future)


## Conda releases

`ploomber` is available in conda (via [conda-forge](https://conda-forge.org/)). The recipes are located here:

* [ploomber](https://github.com/conda-forge/ploomber-feedstock)
* [ploomber-scaffold](https://github.com/conda-forge/ploomber-scaffold-feedstock)

The first feedstock corresponds to the core package, and the second is a complimentary package that implements the scaffolding logic (i.e., `ploomber scaffold`). When uploading a new version to PyPI, the conda-forge bot automatically opens a PR to the feedstocks; upon approval, the new versions are available to install via `conda install ploomber --channel conda-forge`.

Note that conda-forge implements a CI pipeline that checks that the recipe works. Thus, under most circumstances, the PR will pass. One exception is when adding new dependencies to `setup.py`; in such a case, we must manually edit the recipe (`meta.yml`) and open a PR to the feedstock.

Note that [it takes some time](https://conda-forge.org/docs/maintainer/maintainer_faq.html#mfaq-anaconda-delay) for packages to be available for download. You can verify successful upload by opening Anaconda.org ([ploomber](https://anaconda.org/conda-forge/ploomber), [ploomber-scaffold](https://anaconda.org/conda-forge/ploomber-scaffold)); such website is updated immediately.

To check if packages are available: `conda search ploomber --channel cf-staging`. Pending packages will appear in channel [`cf-staging`](https://conda-forge.org/docs/maintainer/infrastructure.html#output-validation-and-feedstock-tokens) while available packages in `conda-forge`. It usually takes less than one hour for packages to move from one to the other.