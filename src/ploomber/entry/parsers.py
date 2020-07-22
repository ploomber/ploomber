import logging
import sys
import importlib
import inspect
from pathlib import Path
import argparse
from collections.abc import Mapping

import yaml

from ploomber.spec.DAGSpec import DAGSpec
from ploomber.env.EnvDict import EnvDict


class CustomParser(argparse.ArgumentParser):
    """
    A custom ArgumentParser that keeps track of arguments
    """
    def __init__(self, *args, **kwargs):
        self.static_args = []
        self.finished_static_api = False
        super().__init__(*args, **kwargs)

        self.add_argument('--log',
                          '-l',
                          help='Enables logging to stdout at the '
                          'specified level',
                          default=None)

    def add_argument(self, *args, **kwargs):
        if not self.finished_static_api:
            self.static_args.extend([arg.replace('-', '') for arg in args])
        return super().add_argument(*args, **kwargs)

    def done_with_static_api(self):
        self.finished_static_api = True


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
    parameters = {
        p.name: {
            'desc': ' '.join(p.desc),
            'type': p.type
        }
        for p in doc['Parameters']
    }
    summary = doc['Summary']
    return {'params': parameters, 'summary': summary}


def _args_to_replace_in_env(args, static_args):
    """
    Returns a dictionary with all extra parameters passed, all these must
    be parameters to replace env values
    """
    return {
        name: getattr(args, name)
        for name in dir(args) if not name.startswith('_')
        if getattr(args, name) is not None if name not in static_args
    }


def _add_args_from_env_dict(parser, env_dict):
    """
    Add one parameter to the args parser by taking a look at all values
    defined in an env dict object
    """
    flat_env_dict = _flatten_dict(env_dict._data)
    for arg, val in flat_env_dict.items():
        parser.add_argument('--env__' + arg, help='Default: {}'.format(val))


def _parse_module(s):
    parts = s.split('.')

    if len(parts) < 2:
        raise ImportError('Invalid module name, must be a dot separated '
                          'string, with at least '
                          '[module_name].[function_name]')

    return '.'.join(parts[:-1]), parts[-1]


def _parse_signature_from_callable(callable_):
    """
    Parse a callable signature, return a dictionary with
    {param_key: default_value} and a list of required parameters
    """
    sig = inspect.signature(callable_)

    required = [
        k for k, v in sig.parameters.items() if v.default == inspect._empty
    ]

    defaults = {
        k: v.default
        for k, v in sig.parameters.items() if v.default != inspect._empty
    }

    return required, defaults


def get_desc(doc, arg):
    arg_data = doc['params'].get(arg)
    return None if arg_data is None else arg_data['desc']


def _add_args_from_callable(parser, callable_):
    """
    Modifies an args parser to include parameters from a callable, adding
    parameters with default values as optional and parameters with no defaults
    as mandatory. Adds descriptions from parsing the callable's docstring

    Returns parsed args: required (list) and defaults (dict)
    """
    doc = _parse_doc(callable_.__doc__)
    required, defaults = _parse_signature_from_callable(callable_)

    for arg, default in defaults.items():
        parser.add_argument('--' + arg, help=get_desc(doc, arg))

    for arg in required:
        parser.add_argument(arg, help=get_desc(doc, arg))

    return required, defaults


def _process_file_entry_point(parser, entry_point, static_args):
    """
    Process a file entry point, returns the initialized dag and parsed args
    """
    if Path('env.yaml').exists():
        env_dict = EnvDict('env.yaml')
        _add_args_from_env_dict(parser, env_dict)

    args = parser.parse_args()

    if hasattr(args, 'log'):
        if args.log is not None:
            logging.basicConfig(level=args.log.upper())

    with open(entry_point) as f:
        dag_dict = yaml.load(f, Loader=yaml.SafeLoader)

    if Path('env.yaml').exists():
        env = EnvDict('env.yaml')
        replaced = _args_to_replace_in_env(args, static_args)
        env = env._replace_flatten_keys(replaced)
        dag = DAGSpec(dag_dict, env=env).to_dag()
    else:
        dag = DAGSpec(dag_dict).to_dag()

    return dag, args


def _process_factory_entry_point(parser, entry_point, static_args):
    """Parse a factory entry point, returns initialized dag and parsed args
    """
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
                             '"{}", make sure it is a valid callable'.format(
                                 name, mod)) from e

    required, _ = _add_args_from_callable(parser, entry)

    # if entry point was decorated with @with_env, add arguments
    # to replace declared variables in env.yaml
    if hasattr(entry, '_env_dict'):
        _add_args_from_env_dict(parser, entry._env_dict)

    args = parser.parse_args()

    if hasattr(args, 'log'):
        if args.log is not None:
            logging.basicConfig(level=args.log.upper())

    # required by the function signature
    kwargs = {key: getattr(args, key) for key in required}

    # env and function defaults replaced
    replaced = _args_to_replace_in_env(args, static_args)

    # TODO: add a way of test this by the parameters it will use to
    # call the function, have an aux function to get those then another
    # to execute, test using the first one
    dag = entry(**{**kwargs, **replaced})

    return dag, args


def _process_entry_point(parser, entry_point, static_args):
    """
    Process an entry point (either a file or a dotted path to a factory)
    """
    # first check if the entry point is an existing file
    if Path(entry_point).exists():
        dag, args = _process_file_entry_point(parser, entry_point, static_args)
    # assume it's a dotted path to a factory
    else:
        dag, args = _process_factory_entry_point(parser, entry_point,
                                                 static_args)

    return dag, args


def _custom_command(parser):
    """
    Parses an entry point, adding arguments by extracting them from the env.
    Returns a dag and the parsed args
    """
    # we add args dynamically based on positional arguments, we need
    # to know how many we got
    n_positional = len([arg for arg in sys.argv if not arg.startswith('-')])

    # this happens when calling python -m ploomber.entry
    if n_positional <= 1:
        # just parse args, this is needed to make --help work
        parser.parse_args()
    # we got an actual positional argument parse entry_point
    else:
        entry_point = sys.argv[1]

        dag, args = _process_entry_point(parser, entry_point,
                                         parser.static_args)
        return dag, args


def _flatten_dict(d, prefix=''):
    """
    Convert a nested dict: {'a': {'b': 1}} -> {'a__b': 1}
    """
    out = {}

    for k, v in d.items():
        if isinstance(v, Mapping):
            out = {**out, **_flatten_dict(v, prefix=prefix + k + '__')}
        else:
            out[prefix + k] = v

    return out
