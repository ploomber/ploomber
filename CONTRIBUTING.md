# Contributing to Ploomber

Thanks for considering contributing to Ploomber!

For general information, see [Ploombers' contribution guidelines.](https://github.com/ploomber/contributing/blob/main/CONTRIBUTING.md)

Issues tagged with [good first issue](https://github.com/ploomber/ploomber/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) are great options to start contributing.

If you get stuck, [open an issue](https://github.com/ploomber/ploomber/issues/new?title=CONTRIBUTING.md%20issue) or reach out to us on [Slack](https://ploomber.io/community/) and we'll happily help you.

If you're contributing to the documentation, go to [doc/CONTRIBUTING.md](doc/CONTRIBUTING.md).

## Setup with conda

The easiest way to setup the development environment is via the setup command; you must have miniconda installed. If you don't want to use conda, skip to the next section.

[Click here for miniconda installation details](https://docs.conda.io/en/latest/miniconda.html).

Make sure conda has conda-forge as channel, running the following:

```sh
conda config --add channels conda-forge
```

Once you have conda ready:

Fork the repository to your account by clicking fork button

<p align="center" width="100%">
  <img src="_static/fork.png" height="40">
</p>

Now ready to clone and setup the environment:


```sh
# get the code
git clone https://github.com/ploomber/ploomber

# invoke is a library we use to manage one-off commands
pip install invoke

# move into ploomber directory
cd ploomber

# setup development environment
invoke setup
```

*Note:* If you're using Linux, you may encounter issues running `invoke setup` regarding the `psycopg2` package. If that's the case, remove `psycopg2` from the `setup.py` file and try again.

Then activate the environment:

```sh
conda activate ploomber
```

## Setup with pip

Ploomber has optional features that depend on packages that aren't straightforward to install, so we use `conda` for quickly setting up the development environment. But you can still get a pretty good development environment using `pip` alone.

### [Optional] Create virtual environment

**Note**: we highly recommend you to install ploomber in a virtual environment (the most straightforward alternative is the [venv](https://docs.python.org/3/library/venv.html) built-in module):

```sh
# create virtual env
python -m venv ploomber-venv

# activate virtual env (linux/macOS)
source ploomber-venv/bin/activate

# activate virtual env (windows)
.\ploomber-venv\Scripts\activate
```
*Note:* [Check venv docs](https://docs.python.org/3/library/venv.html#creating-virtual-environments) to find the appropriate command if you're using Windows.

### Install dependencies

```sh
# required to run the next command
pip install invoke

# install dependencies with pip
invoke setup-pip
```

*Note:* If you're using Linux, you may encounter issues running `invoke setup` regarding the `psycopg2` package. If that's the case, remove `psycopg2` from the `setup.py` file and try again.

### Caveats of installing with pip

Conda takes care of installing all dependencies required to run all tests. However, we need to skip a few of them when installing with pip because either the library is not pip-installable or any of their dependencies are. So if you use `invoke setup-pip` to configure your environment, some tests will fail. This isn't usually a problem if you're developing a specific feature; you can run a subset of the testing suite and let GitHub run the entire test suite when pushing your code.

However, if you wish to have a full setup, you must install the following dependencies:

1. [pygrapviz](https://github.com/pygraphviz/pygraphviz) (note that this depends on [graphviz](https://graphviz.org/)) which can't be installed by pip
2. [IRKernel](https://github.com/IRkernel/IRkernel) (note that this requires an R installation)

## Checking setup

Make sure everything is working correctly:

```sh
# import ploomber
python -c 'import ploomber; print(ploomber)'
```

*Note:* the output of the previous command should be the directory where you ran `git clone`; if it's not, try re-activating your conda environment (i.e., if using conda: `conda activate base`, then `conda activate ploomber`) If this doesn't work, [open an issue](https://github.com/ploomber/ploomber/issues/new?title=CONTRIBUTING.md%20issue) or reach out to us on [Slack](https://ploomber.io/community/).


Run some tests:

```
pytest tests/util
```

## Branch name requirement

To prevent double execution of the same CI pipelines, we have chosen to set a limitation to github push event. Only pushes to certain branches will trigger the pipelines. That means if you have turned on github action and want to run workflows in your forked repo, you will need to either make pushes directly to your master branch or branches name strictly following this convention: `dev/{your-branch-name}`.

On the other hand, if you choose not to turn on github action in your own repo and simply run tests locally, you can disregard this information since your pull request from your forked repo to ploomber/ploomber repo will always trigger the pipelines. 

## Linting

*Note: ploomber/ploomber is the only project where we use yapf, other projects have moved to black*


We use [yapf](https://github.com/google/yapf) for formatting code. *Please run yapf on your code before submitting*:

```sh
yapf --in-place path/to/file.py
```

If you want git to automatically check your code with `flake8` before you push to your fork, you can install a pre-push hook locally:

```sh
# to install pre-push git hook
invoke install-git-hook

# to uninstall pre-push git hook
invoke uninstall-git-hook
```

The installed hook only takes effect in your current repository.

## Testing

* Ploomber loads user's code dynamically via dotted paths (e.g., `my_module.my_function` is similar to doing `from my_module import my_function`). Hence, some of our tests do this as well. Dynamic imports can become a problem if tests create and import modules (i.e., create a new `.py` file and import it). To prevent temporary modules from polluting other tasks, use the `tmp_imports` pytest fixture, which deletes all packages imported inside a test
* Some tests make calls to a PostgreSQL database. When running on Github Actions, a database is automatically provisioned, but the tests will fail locally.
* If you're checking error messages and they include absolute paths to files, you may encounter some issues when running the Windows CI since the Github Actions VM has some symlinks. If the test calls `Pathlib.resolve()` ([resolves symlinks](https://docs.python.org/3/library/pathlib.html#id5)), call it in the test as well, if it doesn't, use `os.path.abspath()` (does not resolve symlinks).

## Locally running GitHub actions

Debugging GitHub actions by commiting, pushing, and then waiting for GitHub to 
run them can be inconvenient because of the clunky workflow and inability to
use debugging tools other than printing to the console

We can use the tool [`act`](https://github.com/nektos/act) to run github 
actions locally in docker containers

Install then run `act` in the root directory. On the first invocation it will
ask for a size. Select medium. `act` will then run actions from the 
`.github/workflows` directory

#### Working with containers

If the tests fail, act will leave the docker images after the action finishes.
These can be inspected by running `docker container list` then running
`docker exec -it CONTAINER_ID bash` where `CONTAINER_ID` is a container id
from `docker container list`

To install packages in the container, first run `apt-get update`. Packages
can be installed normally with apt after


## Ok-to-test

We have separated our tests to unit tests and integration tests. Some of the integration tests may require certain secrets or credentials to run, to prevent such sensitive data from leaking, we have migrated the [ok-to-test](https://github.com/imjohnbo/ok-to-test) template into our CI. 

In short, when you start a pull request from your forked repo, only the workflow for unit tests that don't require any special secrets will run. The integration tests in your pull request check will display skipped. To run the integration tests, please ask one of the maintainers of ploomber to comment `/ok-to-test sha={#commit}` on your pull request where the #commit is the first seven digits of the latest commit of your branch.




