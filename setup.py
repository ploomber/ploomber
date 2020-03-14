#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# based on: https://blog.ionelmc.ro/2014/05/25/python-packaging/
import io
import re
import ast
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import find_packages
from setuptools import setup


_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('src/ploomber/__init__.py', 'rb') as f:
    VERSION = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))


def read(*names, **kwargs):
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ).read()


# matplotlib only needed for dag.plot(output='matplotlib'),
PLOT = ['matplotlib', 'pygraphviz']

# NOTE: most users just do "pip install jupyter" which installs everything
# needed to run papermill but we don't strictly need the whole thing and
# we have to pin specific versions of jupyter_client, nbconvert and
# ipykernel are pinned to support parallel execution using papermill
# (avoid "kernel did not respond" errors)
# these are versions are not pinned in papermill (yet) so we put it here
# more info:
# https://discourse.jupyter.org/t/nbconvert-5-6-0-release/1867
# https://github.com/nteract/papermill/issues/239
NB = ['papermill', 'jupytext', 'ipykernel>=1.5.2',
      'jupyter_client>=5.3.1', 'nbconvert>=5.6.0']
MISC = [
    # sql dumps
    'pandas',
    # parquet support
    'pyarrow',
    # RemoteShellClient
    'paramiko',
]

setup(
    name='ploomber',
    version=VERSION,
    license='A license',
    description='A Python library for developing great data pipelines',
    long_description='%s\n%s' % (
        re.compile('^.. start-badges.*^.. end-badges',
                   re.M | re.S).sub('', read('README.rst')),
        re.sub(':[a-z]+:`~?(.*?)`', r'``\1``', read('CHANGELOG.rst'))
    ),
    author='',
    author_email='',
    url='https://github.com/ploomber/ploomber',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    install_requires=[
        'pyyaml', 'networkx', 'jinja2', 'tabulate',  'humanize', 'tqdm',
        'numpydoc', 'importlib_resources;python_version<"3.7"',
        # for code normalization
        'sqlparse', 'autopep8', 'parso',
        # for generating dag.to_markup(fmt='html')
        'mistune',
        'sqlalchemy',
    ],
    extras_require={
        'plot': PLOT,
        'nb': NB,
        'all': MISC + PLOT + NB,
    },
    entry_points={
        'console_scripts': [
        ]
    },
)
