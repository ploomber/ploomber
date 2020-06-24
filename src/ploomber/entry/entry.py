"""
The entry module automatically creates command line interfaces from functions
that return a DAG object.

Example:

Assume module.sub_module.entry_point is a function that returns a DAG.

Run this will parse the function parameters and return them:

    ``$ python -m ploomber.entry module.sub_module.entry_point --help``


If numpydoc is installed, it will parse the docstring (if any) and add it
when using --help

To build the dag returned by the entry point:

    ``$ python -m ploomber.entry module.sub_module.entry_point``


To start an interactive session by loading the entry point first:

    ``$ python -i -m ploomber.entry module.sub_module.entry_point --action status``

``ipython -i -m`` also works. Once the interactive session starts, the object
returned by the entry point will be available in the "dag" variable.

Features:

    * Parse docstring and show it using ``--help``
    * Enable logging to standard output using ``--log LEVEL``
    * Pass function parameters using ``--PARAM_NAME VALUE``
    * If the function is decorated with ``@with_env``, replace env variables using ``--env__KEY VALUE``


``ploomber.entry`` only works with the factory pattern (a function that returns
a dag). But this same pattern can be applied to start interactive sesssions
if you have your own CLI logic (note we are calling the module directly instead
of using ploomber.entry):

    ``$ python -i -m module.sub_module.entry_point --some_arg``


In IPython:

    ``$ ipython -i -m module.sub_module.entry_point -- --some_arg``


Note: you need to add ``--`` to prevent IPython from parsing your custom args

"""
# TODO: print enviornment content on help and maybe on any other command
# it is useful for debugging purposes
import logging
import sys
import importlib
import argparse
import inspect
from pathlib import Path
from collections.abc import Mapping

import yaml

from ploomber.spec.DAGSpec import init_dag


def _parse_doc(doc):
    """
    Convert numpydoc docstring to a list of dictionaries
    """
    # no docstring
    if doc is None:
        return {'params': {}, 'summary': None}

    # try to import numpydoc
    docscrape = importlib.import_module('numpydoc.docscrape')

    if not docscrape:
        return {'params': {}, 'summary': None}

    doc = docscrape.NumpyDocString(doc)
    parameters = {p.name: {'desc': ' '.join(p.desc), 'type': p.type}
                  for p in doc['Parameters']}
    summary = doc['Summary']
    return {'params': parameters, 'summary': summary}


def _parse_module(s):
    parts = s.split('.')

    if len(parts) < 2:
        raise ImportError('Invalid module name, must be a dot separated '
                          'string, with at least '
                          '[module_name].[function_name]')

    return '.'.join(parts[:-1]), parts[-1]


def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument('entry_point', help='Entry point (DAG)')
    parser.add_argument('--log', help='Enables logging to stdout at the '
                        'specified level', default=None)
    parser.add_argument('--action', help='Action to execute, defaults to '
                        'build', default='build')

    n_positional = len([arg for arg in sys.argv if not arg.startswith('-')])

    if n_positional < 2:
        args = parser.parse_args()
    else:
        # parse entry_point
        entry_point = sys.argv[1]

        if Path(entry_point).exists():
            args = parser.parse_args()

            if args.log is not None:
                logging.basicConfig(level=args.log)

            with open(entry_point) as f:
                dag_dict = yaml.load(f, Loader=yaml.SafeLoader)

            dag = init_dag(dag_dict)
            getattr(dag, args.action)()
            return dag

        else:

            mod, name = _parse_module(entry_point)

            try:
                module = importlib.import_module(mod)
            except ImportError as e:
                raise ImportError('An error happened when trying to '
                                  'import module "{}"'.format(mod)) from e

            try:
                entry = getattr(module, name)
            except AttributeError as e:
                raise AttributeError('Could not get attribute "{}" from module '
                                     '"{}", make sure it is a valid callable'
                                     .format(name, mod)) from e

            doc = _parse_doc(entry.__doc__)

            def get_desc(arg):
                arg_data = doc['params'].get(arg)
                return None if arg_data is None else arg_data['desc']

            sig = inspect.signature(entry)

            defaults = {k: v.default for k, v in sig.parameters.items()
                        if v.default != inspect._empty}
            required = [k for k, v in sig.parameters.items()
                        if v.default == inspect._empty]

            for arg, default in defaults.items():
                parser.add_argument('--'+arg,
                                    help=get_desc(arg))

            for arg in required:
                parser.add_argument(arg, help=get_desc(arg))

            # if entry point was decorated with @with_env, add arguments
            # to replace declared variables in env.yaml
            if hasattr(entry, '_env_dict'):
                flat_env_dict = _flatten_dict(entry._env_dict._data)
                for arg, val in flat_env_dict.items():
                    parser.add_argument('--env__'+arg,
                                        help='Default: {}'.format(val))

            args = parser.parse_args()

            if args.log is not None:
                logging.basicConfig(level=args.log)

            # required by the function signature
            kwargs = {key: getattr(args, key) for key in required}

            # env and function defaults replaced
            replaced = {name: getattr(args, name)
                        for name in dir(args)
                        if not name.startswith('_')
                        if getattr(args, name) is not None
                        if name not in {'entry_point', 'action', 'log'}}

            # TODO: add a way of test this by the parameters it will use to
            # call the function, have an aux function to get those then another
            # to execute, test using the first one
            obj = entry(**{**kwargs, **replaced})

            print(getattr(obj, args.action)())

            return obj


def _flatten_dict(d, prefix=''):
    """
    Convert a nested dict: {'a': {'b': 1}} -> {'a__b': 1}
    """
    out = {}

    for k, v in d.items():
        if isinstance(v, Mapping):
            out = {**out, **_flatten_dict(v, prefix=prefix + k + '__')}
        else:
            out[prefix+k] = v

    return out
