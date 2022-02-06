# Contributing to Ploomber's documentation

## Setup with conda

The simplest way to setup the environment is via conda. [Click here for miniconda installation details](https://docs.conda.io/en/latest/miniconda.html).


```sh
# invoke is needed to run the next command
pip install invoke

# install dev + doc dependencies
invoke setup --doc
```

Then activate the environment:

```sh
conda activate ploomber
```

## Setup with pip

If you don't want to install conda, you can setup the environment with pip:

*Note* we highly recommend you to install dependencies in a virtual environment. See the *Setup with pip* section in the main [`CONTRIBUTING.md`](../CONTRIBUTING.md) file for details.

```sh
# invoke is needed to run the next command
pip install invoke

# install dev + doc dependencies
invoke setup-pip --doc
```

*Note* there are some minor caveats when installing using git, see the *Caveats of installing with pip* in the main [`CONTRIBUTING.md`](../CONTRIBUTING.md) file for details.

## Build docs

```
invoke docs
```

To see the docs, open `doc/_build/html/index.html`


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

Some pages in the documentation are pulled from a separate repository, you can identify them because they're Jupyter notebooks ([here's an example](https://docs.ploomber.io/en/latest/get-started/spec-api-python.html)), if you want to contribute to one of those, you open your Pull Request in the [projects repository](https://github.com/ploomber/projects).

If such notebooks link to other documents, use relative paths, so they work locally as well:

e.g.:

```md
[some-label](../api/testing.rst)
```

[Reference](https://nbsphinx.readthedocs.io/en/0.7.1/markdown-cells.html#Links-to-*.rst-Files-(and-Other-Sphinx-Source-Files)).
