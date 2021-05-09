#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import re
import ast
from glob import glob
from os.path import basename
from os.path import splitext
from pathlib import Path

from setuptools import find_packages
from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('src/ploomber/__init__.py', 'rb') as f:
    VERSION = str(
        ast.literal_eval(
            _version_re.search(f.read().decode('utf-8')).group(1)))

here = Path(__file__).parent.resolve()


def read(name):
    return Path(here, name).read_text()


# NOTE: most users just do "pip install jupyter" but
# we have to pin specific versions of jupyter_client, nbconvert and
# ipykernel to support parallel execution using papermill
# (avoid "kernel did not respond" errors)
# these are versions are not pinned in papermill (yet) so we put it here
# more info:
# https://discourse.jupyter.org/t/nbconvert-5-6-0-release/1867
# https://github.com/nteract/papermill/issues/239
NB = [
    'papermill',
    'jupytext',
    'ipykernel>=1.5.2',
    'jupyter_client>=5.3.1',
    'nbconvert>=5.6.0',
    'notebook',
    'nbformat',
    # for notebook validation
    'pyflakes',
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
    'pandas',
    # for ParquetIO
    'pyarrow',
    # qa and entry modules
    'numpydoc',
]

TESTING = [
    # plotting. strictly speaking pygrapviz is an optional dependency but we
    # don't add it as such because it's gonna break installation for most
    # setups, since we don't expect users to have graphviz installed
    'pygraphviz',
    # RemoteShellClient
    'paramiko',
    # Upload to S3
    'boto3',
    # testing uplaod to S3 task
    'moto',
    # Upload to google cloud storage
    'google-cloud-storage',
    'pytest',
    'pytest-cov',
    # TODO: update config so coveralls 3 works
    'coveralls<3',
    # we need this because we are re-using the original jupyter test suite for
    # testing our contents manager (which imports nose), see test_jupyter.py
    'nose',
    'yapf',
    'flake8',
    # needed to run some test pipelines
    'matplotlib',
    'seaborn',
    'psycopg2',
    # requires for some Table tests where we parse the HTML repr
    'lxml',
]

# packages needed for development
DEV = ['twine']

DESCRIPTION = (
    'A convention over configuration workflow orchestrator. Develop locally '
    '(Jupyter or your favorite editor), deploy to Airflow or Kubernetes. ')

setup(
    name='ploomber',
    version=VERSION,
    description=DESCRIPTION,
    long_description=read('README.md') + '\n' + read('CHANGELOG.md'),
    long_description_content_type='text/markdown',
    author='Eduardo Blancas',
    author_email='hello@ploomber.io',
    url='https://github.com/ploomber/ploomber',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    data_files=[
        # like `jupyter serverextension enable --sys-prefix`
        ("etc/jupyter/jupyter_notebook_config.d",
         ["jupyter-config/jupyter_notebook_config.d/ploomber.json"])
    ],
    zip_safe=False,
    classifiers=[
        # https://pypi.org/classifiers/
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Unix',
        'Operating System :: POSIX',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    install_requires=[
        'ploomber-scaffold',
        'pyyaml',
        'networkx',
        'jinja2',
        'tabulate',
        'humanize',
        'tqdm',
        'importlib_resources;python_version<"3.7"',
        # for code normalization, parso is also needed for inferring upstream
        # dependencies in jupyter notebooks
        'sqlparse',
        'autopep8',
        'parso',
        # for generating dag.to_markup(fmt='html')
        'mistune',
        # for syntax highlighting when generating dag HTML reports
        'pygments',
        'sqlalchemy',
        # for cli
        'click',
        # for ploomber interact and {PythonCallable, NotebookRunner}.debug()
        'ipython',
        'ipdb',
        'pydantic',
    ] + NB,
    extras_require={
        'all': OPTIONAL,
        'dev': OPTIONAL + TESTING + DEV,
    },
    entry_points={
        'console_scripts': ['ploomber=ploomber.cli.cli:cmd_router'],
    },
)
