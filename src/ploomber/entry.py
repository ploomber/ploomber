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

from numpydoc.docscrape import NumpyDocString

# TODO: move this here
from ploomber.env.EnvDict import flatten_dict


def parse_doc(doc):
    """
    Convert numpydoc docstring to a list of dictionaries
    """
    doc = NumpyDocString(doc)
    parameters = {p.name: {'desc': ' '.join(p.desc), 'type': p.type}
                  for p in doc['Parameters']}
    summary = doc['Summary']
    return {'params': parameters, 'summary': summary}


def main():
    parser = argparse.ArgumentParser()

    n_pos = len([arg for arg in sys.argv if not arg.startswith('-')])

    # print(n_pos)
    # print(sys.argv)

    parser.add_argument('entry_point', help='Entry point (DAG)')

    if n_pos < 2:
        args = parser.parse_args()
    else:
        parser.add_argument('action', help='Action to execute')
        parts = sys.argv[1].split('.')
        mod, name = '.'.join(parts[:-1]), parts[-1]

        entry = getattr(importlib.import_module(mod), name)
        flat_env_dict = flatten_dict(entry._env_dict)

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
            parser.add_argument('--'+arg, help='Default: {}'.format(val))

        args = parser.parse_args()

        kwargs = {key: getattr(args, key) for key in required}

        print(getattr(entry(**kwargs), args.action)())


if __name__ == '__main__':
    main()
