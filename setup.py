#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import re
import ast
from pathlib import Path

from setuptools import find_packages
from setuptools import setup

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("src/ploomber/__init__.py", "rb") as f:
    VERSION = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )

here = Path(__file__).parent.resolve()


def read(name):
    return Path(here, name).read_text(encoding="utf-8")


# NOTE: most users just do "pip install jupyter" but
# we have to pin specific versions of jupyter_client, nbconvert and
# ipykernel to support parallel execution using papermill
# (avoid "kernel did not respond" errors)
# these are versions are not pinned in papermill (yet) so we put it here
# more info:
# https://discourse.jupyter.org/t/nbconvert-5-6-0-release/1867
# https://github.com/nteract/papermill/issues/239
NB = [
    "papermill",
    "notebook<7",
    "jupytext",
    "ipykernel>=1.5.2",
    "jupyter_client>=5.3.1",
    "nbconvert>=5.6.0",
    "nbformat",
    # for notebook validation
    "pyflakes",
]

# Optional dependencies are packages that are used in several modules but are
# not strictly required. Dependencies that are required for a single use case
# (e.g. upload to s3) should be included in the "TESTING" list. Both optional
# and one-time modules should use the @requires decorator to show an error if
# the dependency is missing. numpydoc is an special case because it's an
# optional dependency but not having it installed does not trigger an error
# it will just not print the parsed docstring.
OPTIONAL = [
    # sql dumps and uploads
    "pandas",
    # for ParquetIO
    "pyarrow",
    # qa and entry modules
    "numpydoc",
    # for embedded dag plots with d3 backend
    "requests-html",
    "nest_asyncio",
]

TESTING = [
    # plotting. strictly speaking pygrapviz is an optional dependency but we
    # don't add it as such because it's gonna break installation for most
    # setups, since we don't expect users to have graphviz installed
    'pygraphviz;python_version<"3.10"',
    # RemoteShellClient
    "paramiko",
    # Upload to S3
    "boto3",
    # testing upload to S3 task
    "moto",
    # Upload to google cloud storage
    "google-cloud-storage",
    # NOTE: pytest introduced some breaking changes
    "pytest==7.1.*",
    "pytest-cov",
    # TODO: update config so coveralls 3 works
    "coveralls<3",
    # we need this because we are re-using the original jupyter test suite for
    # testing our contents manager (which imports nose), see test_jupyter.py
    "nose",
    "yapf",
    "flake8",
    # needed to run some test pipelines
    "matplotlib",
    "seaborn",
    # https://www.psycopg.org/docs/install.html#psycopg-vs-psycopg-binary
    # this one is easier to install
    "psycopg2-binary",
    # requires for some Table tests where we parse the HTML repr
    "lxml",
    # for testing jupyter lab plugin
    "jupyter_server",
    "notebook",
    # optional dependencies for @serializer and @unserializer
    "joblib",
    "cloudpickle",
    # for testing the webpdf converter
    "nbconvert[webpdf]",
    # for testing ParallelDill,
    "multiprocess",
    # dill 0.3.6 is breaking windows github actions
    "dill==0.3.5.1",
    # pandas not yet compatible with sqlalchemy 2
    "sqlalchemy<2",
]

# packages needed for development
DEV = ["twine", "invoke", "pkgmt"]

DESCRIPTION = (
    "Write maintainable, production-ready pipelines using Jupyter or your "
    "favorite text editor. Develop locally, deploy to the cloud."
)

setup(
    name="ploomber",
    version=VERSION,
    description=DESCRIPTION,
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Ploomber",
    author_email="contact@ploomber.io",
    url="https://github.com/ploomber/ploomber",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    data_files=[
        (
            "etc/jupyter/jupyter_notebook_config.d",
            ["jupyter-config/jupyter_notebook_config.d/ploomber.json"],
        ),
        (
            "etc/jupyter/jupyter_server_config.d",
            ["jupyter-config/jupyter_server_config.d/ploomber.json"],
        ),
    ],
    zip_safe=False,
    classifiers=[
        # https://pypi.org/classifiers/
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    install_requires=[
        "ploomber-scaffold>=0.3",
        # added fix to manage the IPython terminal singleton
        "ploomber-engine>=0.0.8",
        # added @deprecated.method
        "ploomber-core>=0.0.11",
        "pyyaml",
        "networkx>=2.5",
        "jinja2",
        "tabulate",
        "humanize",
        "tqdm",
        "posthog",
        # for code normalization, parso is also needed for inferring upstream
        # dependencies in jupyter notebooks
        "sqlparse",
        "autopep8",
        "parso",
        # for generating dag.to_markup(fmt='html')
        "mistune",
        # for syntax highlighting when generating dag HTML reports
        "pygments",
        "sqlalchemy",
        # for cli
        "click",
        # for ploomber interact and {PythonCallable, NotebookRunner}.debug()
        "ipython",
        "ipdb",
        "pydantic",
    ]
    + NB,
    extras_require={
        "all": OPTIONAL,
        "dev": OPTIONAL + TESTING + DEV,
    },
    entry_points={
        "console_scripts": ["ploomber=ploomber_cli.cli:cmd_router"],
    },
)
