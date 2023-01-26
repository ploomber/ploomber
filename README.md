<p align="center" width="100%">
  <img src="_static/logo.png" height="250">
</p>

[![CI Linux](https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg)
[![CI macOS](https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg)
[![CI Windows](https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/ploomber/badge/?version=latest)](https://docs.ploomber.io/en/latest/?badge=latest)
[![PyPI](https://badge.fury.io/py/ploomber.svg)](https://badge.fury.io/py/ploomber)
[![Conda (channel only)](https://img.shields.io/conda/vn/conda-forge/ploomber)](https://anaconda.org/conda-forge/ploomber)
[![Conda](https://img.shields.io/conda/pn/conda-forge/ploomber)](https://anaconda.org/conda-forge/ploomber)
[![Coverage](https://coveralls.io/repos/github/ploomber/ploomber/badge.svg?branch=master)](https://coveralls.io/github/ploomber/ploomber?branch=master)
[![Twitter](https://img.shields.io/twitter/follow/ploomber?label=Follow&style=social)](https://twitter.com/intent/user?screen_name=ploomber)
[![Downloads](https://pepy.tech/badge/ploomber)](https://pepy.tech/project/ploomber)

<p align="center">
  <a href="https://ploomber.io/community">Join our community</a>
  |
  <a href="https://share.hsforms.com/1E7Qa_OpcRPi_MV-segFsaAe6c2g">Newsletter</a>
  |
  <a href="mailto:contact@ploomber.io">Contact us</a>
  |
  <a href="https://docs.ploomber.io/">Docs</a>
  |
  <a href="https://ploomber.io/blog/">Blog</a>
  |  
  <a href="https://www.ploomber.io">Website</a>
  |
  <a href="https://www.youtube.com/channel/UCaIS5BMlmeNQE4-Gn0xTDXQ">YouTube</a>
</p>


Ploomber is the fastest way to build data pipelines ⚡️. Use your favorite editor (**[Jupyter](https://docs.ploomber.io/en/latest/user-guide/jupyter.html), [VSCode](https://docs.ploomber.io/en/latest/user-guide/editors.html), [PyCharm](https://docs.ploomber.io/en/latest/user-guide/editors.html)**) to develop interactively and deploy ☁️ without code changes (**[Kubernetes](https://soopervisor.readthedocs.io/en/latest/tutorials/kubernetes.html), [Airflow](https://soopervisor.readthedocs.io/en/latest/tutorials/airflow.html), [AWS Batch](https://soopervisor.readthedocs.io/en/latest/tutorials/aws-batch.html), and [SLURM](https://soopervisor.readthedocs.io/en/latest/tutorials/slurm.html)**). Do you have legacy notebooks? Refactor them into modular pipelines with a single command.

<p align="center">
  <a href="https://binder.ploomber.io/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fguides/intro-to-ploomber%252FREADME.ipynb%26branch%3Dmaster"> <img src="_static/get-started.svg" alt="Get Started"> </a>
</p>


## Installation

*Compatible with Python 3.7 and higher.*

Install with `pip`:

```sh
pip install ploomber
```

Or with `conda`:

```sh
conda install ploomber -c conda-forge
```

## Getting started

### Try the tutorial:
<p>
  <a href="https://binder.ploomber.io/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fguides/first-pipeline%252FREADME.ipynb%26branch%3Dmaster"> <img src="_static/open-in-jupyterlab.svg" alt="Open In JupyterLab"> </a>
</p>

## Community

* [Join us on Slack](https://ploomber.io/community)
* [Newsletter](https://share.hsforms.com/1E7Qa_OpcRPi_MV-segFsaAe6c2g)
* [YouTube](https://www.youtube.com/channel/UCaIS5BMlmeNQE4-Gn0xTDXQ)
* [Contact the development team](mailto:contact@ploomber.io)

## Main Features

### ⚡️ Get started quickly

A simple YAML API to get started quickly, a powerful Python API for total flexibility.

https://user-images.githubusercontent.com/989250/150660813-fc289c6c-0ed5-432d-b6df-063ce98c0093.mp4

### ⏱ Shorter development cycles

Automatically cache your pipeline’s previous results and only re-compute tasks that have changed since your last execution.

https://user-images.githubusercontent.com/989250/150660820-9a3a0abd-5904-492b-97ff-5494285dfebf.mp4

### ☁️ Deploy anywhere

Run as a shell script in a single machine or distributively in [Kubernetes](https://soopervisor.readthedocs.io/en/latest/tutorials/kubernetes.html), [Airflow](https://soopervisor.readthedocs.io/en/latest/tutorials/airflow.html), [AWS Batch](https://soopervisor.readthedocs.io/en/latest/tutorials/aws-batch.html), or [SLURM](https://soopervisor.readthedocs.io/en/latest/tutorials/slurm.html).

https://user-images.githubusercontent.com/989250/150660830-3f81c9a2-5392-49e5-976d-cb8a38441ecb.mp4


### 📙 Automated migration from legacy notebooks

Bring your old monolithic notebooks, and we’ll automatically convert them into maintainable, modular pipelines.

https://user-images.githubusercontent.com/989250/150660840-b0c12f85-504c-4233-8c3d-6724d291f1aa.mp4


[I want to migrate my notebook.](https://docs.ploomber.io/en/latest/user-guide/refactoring.html)

[Show me a demo.](https://www.youtube.com/watch?v=EJecqsZBr3Q)

## Resources

* [Documentation](https://docs.ploomber.io/)
* [PyData Chicago talk (covers motivation and demo)](https://youtu.be/qUL7QabcKcw)
* [Develop and deploy an ML pipeline in 30 minutes (EuroPython 2021)](https://youtu.be/O8tqiCkIWPs)
* [Guest blog post on the official Jupyter blog](https://blog.jupyter.org/ploomber-maintainable-and-collaborative-pipelines-in-jupyter-acb3ad2101a7)
* [Examples (Machine Learning pipeline, ETL, among others)](https://github.com/ploomber/projects)
* [Blog](https://ploomber.io/)
* [Comparison with other tools](https://ploomber.io/posts/survey)
* [More videos](https://docs.ploomber.io/en/latest/videos.html)

## About Ploomber

Ploomber is a big community of data enthusiasts pushing the boundaries of Data Science and Machine Learning tooling.

Whatever your skillset is, you can contribute to our mission. So whether you're a beginner or an experienced professional, you're welcome to join us on this journey!

[Click here to know how you can contribute to Ploomber.](https://github.com/ploomber/contributing/blob/main/README.md)
