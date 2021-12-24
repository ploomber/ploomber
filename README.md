<p align="center" width="100%">
  <img src="_static/logo.png" height="250">
</p>

[![CI Linux](https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg)
[![CI macOS](https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg)
[![CI Windows](https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/ploomber/badge/?version=latest)](https://ploomber.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://badge.fury.io/py/ploomber.svg)](https://badge.fury.io/py/ploomber)
[![Conda (channel only)](https://img.shields.io/conda/vn/conda-forge/ploomber)](https://anaconda.org/conda-forge/ploomber)
[![Conda](https://img.shields.io/conda/pn/conda-forge/ploomber)](https://anaconda.org/conda-forge/ploomber)
[![Coverage](https://coveralls.io/repos/github/ploomber/ploomber/badge.svg?branch=master)](https://coveralls.io/github/ploomber/ploomber?branch=master)
[![Twitter](https://img.shields.io/twitter/follow/ploomber?label=Follow&style=social)](https://twitter.com/intent/user?screen_name=ploomber)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fguides/spec-api-python%252FREADME.ipynb%26branch%3Dmaster)
[![Downloads](https://pepy.tech/badge/ploomber)](https://pepy.tech/project/ploomber)

<p align="center">
  <a href="https://ploomber.io/community">Join our community</a>
  |
  <a href="https://www.getrevue.co/profile/ploomber">Newsletter</a>
  |
  <a href="mailto:contact@ploomber.io">Contact us</a>
  |
  <a href="https://ploomber.readthedocs.io/">Docs</a>
  |
  <a href="https://ploomber.io/">Blog</a>
  |  
  <a href="https://www.ploomber.io">Website</a>
  |
  <a href="https://www.youtube.com/channel/UCaIS5BMlmeNQE4-Gn0xTDXQ">YouTube</a>
</p>

**Notebooks are hard to maintain.** Teams often prototype projects in notebooks, but maintaining them is an error-prone process that slows progress down. Ploomber overcomes the challenges of working with `.ipynb` files allowing teams to develop collaborative, production-ready pipelines using JupyterLab or any text editor.


<p align="center">
  <a href="https://ploomber.readthedocs.io/en/latest/get-started/spec-api-python.html"> <img src="_static/get-started.svg" alt="Get Started"> </a>
  <a href="https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fguides/spec-api-python%252FREADME.ipynb%26branch%3Dmaster"> <img src="_static/open-jupyterlab.svg" alt="Open JupyerLab"> </a>
</p>


## Installation

*Compatible with Python 3.6 and higher.*

Install with `pip`:

```sh
pip install ploomber
```

Or with `conda`:

```sh
conda install ploomber -c conda-forge
```

## Getting started

Use Binder to try out Ploomber without setting up an environment:

[![image](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fguides/spec-api-python%252FREADME.ipynb%26branch%3Dmaster)

Or run an example locally:

```sh
# ML pipeline example
ploomber examples -n templates/ml-basic -o ml-basic
cd ml-basic

# if using pip
pip install -r requirements.txt

# if using conda
conda env create --file environment.yml
conda activate ml-basic

# run pipeline
ploomber build
```

Pipeline output saved in the `output/` folder. Check out the pipeline definition
in the `pipeline.yaml` file.

To get a list of examples, run `ploomber examples`.

Click here to go to our [examples](https://github.com/ploomber/projects) repository.

## Community

* [Join us on Slack](https://ploomber.io/community)
* [Newsletter](https://www.getrevue.co/profile/ploomber)
* [YouTube](https://www.youtube.com/channel/UCaIS5BMlmeNQE4-Gn0xTDXQ)
* [Contact the development team](mailto:contact@ploomber.io)

## Main Features

1. [**Automated notebook refactoring**](https://github.com/ploomber/soorgeon). Automatically convert a legacy notebook into a maintainable, modular pipeline ([see demo](https://www.youtube.com/watch?v=EJecqsZBr3Q)).
2. **Scripts as notebooks.** Open `.py` files as notebooks, then execute them from the terminal and generate an output notebook to review results.
3. **Dependency resolution.** Quickly build a DAG by referring to previous tasks in your code; Ploomber infers execution order and orchestrates execution.
4. **Incremental builds.** Speed up iterations by skipping tasks whose source code hasn't changed since the last execution.
5. **Production-ready.** Deploy to [Kubernetes](https://soopervisor.readthedocs.io/en/latest/tutorials/kubernetes.html) (via Argo Workflows), [Airflow](https://soopervisor.readthedocs.io/en/latest/tutorials/airflow.html), and [AWS Batch](https://soopervisor.readthedocs.io/en/latest/tutorials/aws-batch.html) without code changes.
6. **Parallelization.** Run independent tasks in parallel.
7. **Testing.** Import pipelines in any testing frameworks and test them with any CI service (e.g. GitHub Actions).
8. **Flexible.** Use Jupyter notebooks, Python scripts, R scripts, SQL scripts, Python functions, or a combination of them as pipeline tasks. Write pipelines using a `pipeline.yaml` file or with Python.

![repo-lab-example](https://ploomber.io/images/blog/repo-lab-example.png)


## Resources

* [Documentation](https://ploomber.readthedocs.io/)
* [Develop and deploy an ML pipeline in 30 minutes (EuroPython 2021)](https://youtu.be/O8tqiCkIWPs)
* [Guest blog post on the official Jupyter blog](https://blog.jupyter.org/ploomber-maintainable-and-collaborative-pipelines-in-jupyter-acb3ad2101a7)
* [PyData Chicago talk (covers motivation and demo)](https://youtu.be/qUL7QabcKcw)
* [Examples (Machine Learning pipeline, ETL, among others)](https://github.com/ploomber/projects)
* [Blog](https://ploomber.io/)
* [Comparison with other tools](https://ploomber.io/posts/survey)
* [More videos](https://ploomber.readthedocs.io/en/latest/videos.html)
