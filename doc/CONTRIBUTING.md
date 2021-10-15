# Contributing to Ploomber's documentation

## Setup with conda

The simplest way to setup the environment is via conda. [Click here for installation details](https://docs.conda.io/en/latest/miniconda.html).


```sh
# invoke is a library we use to manage one-off commands
pip install invoke

# setup development environment, add doc dependencies
invoke setup --doc
```

Then activate the environment:

```sh
conda activate ploomber
```

## Setup with pip

If you don't want to install conda, you can setup the environment with pip.
First, follow the "Setup with pip" instructions from the
main [CONTRIBUTING.md](../CONTRIBUTING.md), then install the dependencies in
the `pip` section of the `doc/environment.yml` file (ignore the last entry `-e ..`).
For example:

```sh
pip install joblib scikit-learn
```

## Editing code snippets

To make the docs clear, we distinguish between a Python script and a Python
session by adding CSS to each code snippet, the same happens for bash
scripts and bash sessions. This requires you to use the appropriate Pygments
lexer depending on what you are showing:

https://pygments.org/docs/lexers/

Summary:
* `python` for Python scripts
* `pycon` for Python sessions (Note: add >>> at the beginning of each line)
* `pytb` for Python tracebacks (Note: only works if you copy the whole traceback)
* `sh` For bash scripts
* `console` For terminal sessions
* `postgresql` For SQL templated scripts (`sql` does not work)

If nothing applies, don't add any lexer.

## Pages generated from notebooks

Link to other pages using relative paths so they work locally as well:

e.g.: [some-label](../api/testing.rst)

Reference: https://nbsphinx.readthedocs.io/en/0.7.1/markdown-cells.html#Links-to-*.rst-Files-(and-Other-Sphinx-Source-Files)
