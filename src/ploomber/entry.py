"""
Warning: this code is highly experimental
"""

# TODO: print enviornment content on help and maybe on any other command
# it is useful for debugging purposes
# TODO: this should also work if the function is not decorated with @with_env

import sys
import importlib
import argparse
import inspect
from collections.abc import Mapping

from numpydoc.docscrape import NumpyDocString


# TODO: what to do if numpydoc is not installed? required it? fail silently?
# output  a warning?
def parse_doc(doc):
    """
    Convert numpydoc docstring to a list of dictionaries
    """
    if doc is None:
        return {'params': {}, 'summary': None}

    doc = NumpyDocString(doc)
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


def main():
    parser = argparse.ArgumentParser()

    n_pos = len([arg for arg in sys.argv if not arg.startswith('-')])

    parser.add_argument('entry_point', help='Entry point (DAG)')

    if n_pos < 2:
        args = parser.parse_args()
    else:
        parser.add_argument('action', help='Action to execute')

        mod, name = _parse_module(sys.argv[1])

        try:
            module = importlib.import_module(mod)
        except ImportError as e:
            raise ImportError('Could not import module "{}", '
                              'make sure it is available in the '
                              'current python environment'.
                              format(mod))

        try:
            entry = getattr(module, name)
        except AttributeError as e:
            raise AttributeError('Could not get attribute "{}" from module '
                                 '"{}", make sure such function exists'
                                 .format(mod, name)) from e

        flat_env_dict = flatten_dict(entry._env_dict._data)

        doc = parse_doc(entry.__doc__)

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

        required.remove('env')

        for arg in required:
            parser.add_argument(arg, help=get_desc(arg))

        for arg, val in flat_env_dict.items():
            parser.add_argument('--env__'+arg, help='Default: {}'.format(val))

        args = parser.parse_args()

        # required by the function signature
        kwargs = {key: getattr(args, key) for key in required}

        # env and function defaults replaced
        replaced = {name: getattr(args, name)
                    for name in dir(args)
                    if not name.startswith('_')
                    if getattr(args, name) is not None
                    if name not in {'entry_point', 'action'}}

        # TODO: add a way of test this by the parameters it will use to
        # call the function, have an aux function to get those then another
        # to execute, test using the first one
        print(getattr(entry(**{**kwargs, **replaced}), args.action)())


def flatten_dict(d, prefix=''):
    """
    Convert a nested dict: {'a': {'b': 1}} -> {'a__b': 1}
    """
    out = {}

    for k, v in d.items():
        if isinstance(v, Mapping):
            out = {**out, **flatten_dict(v, prefix=prefix + k + '__')}
        else:
            out[prefix+k] = v

    return out


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error:', e)
        sys.exit(1)
