import logging
import sys
import importlib
import inspect
from pathlib import Path
import argparse
from collections.abc import Mapping

import warnings

from ploomber.spec.dagspec import DAGSpec
from ploomber.env.EnvDict import EnvDict
from ploomber.util.dotted_path import load_dotted_path
from ploomber.util import default


def process_arg(s):
    clean = None

    if s.startswith('--'):
        clean = s[2:]
    elif s.startswith('-'):
        clean = s[1:]
    else:
        clean = s

    return clean.replace('-', '_')


class CustomParser(argparse.ArgumentParser):
    """
    Most of our CLI commands operate on entry points, the CLI signature is
    defined by a few static arguments (they always appear when doing
    ploomber {cmd} --entry-point {entry} --help) and dynamic arguments
    (they are generated depending on the entry point). To support this, we
    have this CustomParser that distinguishes between static and dynamic args

    Once initialized, static args must be added using this as a context
    manager, only after this has happened, other arguments can be added using
    parser.add_argument.
    """
    def __init__(self, *args, **kwargs):
        self.DEFAULT_ENTRY_POINT = default.entry_point()

        self.static_args = []
        self.finished_static_api = False
        self.in_context = False
        self.finished_init = False
        super().__init__(*args, **kwargs)

        self.add_argument('--log',
                          '-l',
                          help='Enables logging to stdout at the '
                          'specified level',
                          default=None)

        self.add_argument(
            '--entry-point',
            '-e',
            help=f'Entry point, defaults to {self.DEFAULT_ENTRY_POINT}',
            default=self.DEFAULT_ENTRY_POINT)

        self.finished_init = True

    def parse_entry_point_value(self):
        """
        Returns the entry_point value pased without calling parse_args(),
        this is required to find env params to show, if we call parse_args()
        the CLI stops there and shows available params
        """
        index = None

        try:
            index = sys.argv.index('--entry-point')
        except ValueError:
            pass

        try:
            index = sys.argv.index('-e')
        except ValueError:
            pass

        return self.DEFAULT_ENTRY_POINT if index is None else sys.argv[index +
                                                                       1]

    def add_argument(self, *args, **kwargs):
        if not self.finished_static_api:
            if not self.in_context and self.finished_init:
                raise RuntimeError('Cannot add arguments until the static '
                                   'API has been declared')
            else:
                self.static_args.extend([process_arg(arg) for arg in args])
        return super().add_argument(*args, **kwargs)

    def __enter__(self):
        self.in_context = True
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.in_context = False
        self.finished_static_api = True

    def process_factory_dotted_path(self, dotted_path):
        """Parse a factory entry point, returns initialized dag and parsed args
        """
        entry = load_dotted_path(str(dotted_path), raise_=True)

        # add args using the function's signature
        required, _ = _add_args_from_callable(self, entry)

        # if entry point was decorated with @with_env, add arguments
        # to replace declared variables in env.yaml
        if hasattr(entry, '_env_dict'):
            _add_cli_args_from_env_dict_keys(self, entry._env_dict)

        args = self.parse_args()

        if hasattr(args, 'log'):
            if args.log is not None:
                logging.basicConfig(level=args.log.upper())

        # extract required (by using function signature) params from the cli
        # args
        kwargs = {key: getattr(args, key) for key in required}

        # env and function defaults replaced
        replaced = _env_keys_to_override(args, self.static_args)

        # TODO: add a way of test this by the parameters it will use to
        # call the function, have an aux function to get those then another
        # to execute, test using the first one
        dag = entry(**{**kwargs, **replaced})

        return dag, args


class EntryPoint:
    """
    Handles common operations on the 4 types of entry points. Exposes a
    pathlib.Path-like interface
    """
    Directory = 'directory'
    Pattern = 'pattern'
    File = 'file'
    DottedPath = 'dotted-path'

    def __init__(self, value):
        self.value = value
        self.type = find_entry_point_type(value)

    def exists(self):
        if self.type == self.Pattern:
            return True
        elif self.type in {self.Directory, self.File}:
            return Path(self.value).exists()
        elif self.type == self.DottedPath:
            return load_dotted_path(self.value, raise_=False) is not None

    def is_dir(self):
        return self.type == self.Directory

    @property
    def suffix(self):
        return None if self.type != self.File else Path(self.value).suffix

    def __repr__(self):
        return repr(self.value)

    def __str__(self):
        return str(self.value)

    def load(self, parser, argv):
        """Load DAG from entry point

        Parameters
        ----------
        parser : CustomParser
            The cli parser object

        argv : list
            Command line arguments
        """
        help_cmd = '--help' in argv or '-h' in argv

        # if the file does not exist but the value has sufix yaml/yml, show a
        # warning because the last thing to try is to interpret it as a dotted
        # path and that's probably not what the user wants
        if not self.exists() and self.suffix in {'.yaml', '.yml'}:
            warnings.warn('Entry point value "{}" has extension "{}", which '
                          'suggests a spec file, but the file doesn\'t '
                          'exist'.format(self, self.suffix))

        # even if the entry file is not a file nor a valid module, show the
        # help menu, but show a warning
        if (help_cmd and not self.exists()):
            warnings.warn('Failed to load entry point "{}". It is not a file '
                          'nor a valid dotted path'.format(self))

            args = parser.parse_args()

        # at this point there are two remaining cases:
        # no help command (entry point may or may not exist),:
        #   we attempt to run the command
        # help command and exists:
        #   we just parse parameters to display them in the help menu
        elif self.type == EntryPoint.DottedPath:
            # if pipeline.yaml, .type will return dotted path, because that's
            # a valid dotted-path value but this can trip users over so we
            # raise a exception here for users to check for name typos
            if str(self.value) in {'pipeline.yaml', 'pipeline.yml'}:
                raise ValueError('Error loading entry point. When passing '
                                 f'{self!r}, a YAML file is expected, but '
                                 'such file does not exist')

            dag, args = parser.process_factory_dotted_path(self)
        else:
            # process file, directory or glob pattern
            dag, args = _process_file_dir_or_glob(parser)

        return dag, args


# TODO: the next two functions are only used to override default behavior
# when using the jupyter extension, but they have to be integrated with the CLI
# to provide consistent behavior. The problem is that logic implemented
# in _process_file_dir_or_glob and _process_factory_dotted_path
# also contains some CLI specific parts that we don't require here
def find_entry_point_type(entry_point):
    """

    Step 1: If not ENTRY_POINT is defined nor a value is passed, a default
    value is used (pipeline.yaml for CLI, recursive lookup for Jupyter client).
    If ENTRY_POINT is defined, this simply overrides the default value, but
    passing a value overrides the default value. Once the value is determined.

    Step 2: If value is a valid directory, DAG is loaded from such directory,
    if it's a file, it's loaded from that file (spec), finally, it's
    interpreted as a dotted path
    """
    if '*' in entry_point:
        return EntryPoint.Pattern
    elif Path(entry_point).exists():
        if Path(entry_point).is_dir():
            return EntryPoint.Directory
        else:
            return EntryPoint.File
    elif '.' in entry_point:
        return EntryPoint.DottedPath
    else:
        raise ValueError(
            'Could not determine the entry point type from value: '
            f'{entry_point!r}. Expected '
            'an existing file, directory glob-like pattern (i.e. *.py) or '
            'dotted path (dot-separated string). Verify your input.')


def load_entry_point(entry_point):
    type_ = find_entry_point_type(entry_point)

    if type_ == EntryPoint.Directory:
        spec = DAGSpec.from_directory(entry_point)
        path = Path(entry_point)

    elif type_ == EntryPoint.File:
        spec = DAGSpec(entry_point)
        path = Path(entry_point).parent
    else:
        raise NotImplementedError(
            f'loading entry point type {type_!r} is unsupported')

    return spec, spec.to_dag(), path


def _first_non_empty_line(doc):
    for line in doc.split('\n'):
        if line:
            return line.strip()


def _parse_doc(callable_):
    """
    Convert numpydoc docstring to a list of dictionaries
    """
    doc = callable_.__doc__

    # no docstring
    if doc is None:
        return {'params': {}, 'summary': None}

    # try to import numpydoc, if can't find it, just returnt the first line
    try:
        docscrape = importlib.import_module('numpydoc.docscrape')
    except ModuleNotFoundError:
        return {'params': {}, 'summary': _first_non_empty_line(doc)}

    doc_parsed = docscrape.NumpyDocString(doc)

    parameters = {
        p.name: {
            'desc': ' '.join(p.desc),
            'type': p.type
        }
        for p in doc_parsed['Parameters']
    }

    # docscrape returns one element per line
    summary = 'Docstring: {}'.format('\n'.join(doc_parsed['Summary']))

    return {'params': parameters, 'summary': summary}


def _env_keys_to_override(args, static_args):
    """
    Returns a dictionary with all extra cli parameters passed, all these must
    be parameters that part of the env or params (with no defaults) if
    entry point is a factory function
    """
    return {
        name: getattr(args, name)
        for name in dir(args) if not name.startswith('_')
        if getattr(args, name) is not None if name not in static_args
    }


def _add_cli_args_from_env_dict_keys(parser, env_dict):
    """
    Add one parameter to the args parser by taking a look at all values
    defined in an env dict object
    """
    # flatten keys from the env dictionary. e.g. from {'a': {'b': 1}} is
    # converted to {'a--b': 1}. This allows us to add cli args such as --a--b
    # to modify any key in the env. Note that we use double hyphens to have
    # an unambious section separator. Environments are commonly loaded from
    # YAML files. Keys in such files might contain hyphens/underscores, we
    # allow users to have those characters but double hyphens/underscores are
    # not permitted as they'd conflict with the CLI generation logic
    flat_env_dict = _flatten_dict(env_dict._data)

    for arg, val in flat_env_dict.items():
        # do not add default keys like {{cwd}}, {{here}}
        if arg not in env_dict.default_keys:
            parser.add_argument('--env--' + arg,
                                help='Default: {}'.format(val))


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

    return required, defaults, sig.parameters


def get_desc(doc, arg):
    arg_data = doc['params'].get(arg)
    return None if arg_data is None else arg_data['desc']


def add_argument_kwargs(params, arg):
    """
    Build kwargs for parser.add_argument function
    """
    valid_hints = [int, float, str, bool]

    fn_default = params[arg].default
    fn_annotation = params[arg].annotation

    # special case, bool with default value becomes a flag
    if fn_annotation is bool and fn_default is not inspect._empty:
        kwargs = {'action': 'store_true' if not fn_default else 'store_false'}
    elif fn_annotation in valid_hints:
        kwargs = {'type': fn_annotation, 'default': fn_default}
    else:
        kwargs = {'default': fn_default}

    return kwargs


def _add_args_from_callable(parser, callable_):
    """
    Modifies an args parser to include parameters from a callable, adding
    parameters with default values as optional and parameters with no defaults
    as mandatory. Adds descriptions from parsing the callable's docstring

    It also adds the description from the docstring, if any

    Returns parsed args: required (list) and defaults (dict)
    """
    doc = _parse_doc(callable_)
    required, defaults, params = _parse_signature_from_callable(callable_)

    for arg in defaults.keys():
        parser.add_argument('--' + arg,
                            help=get_desc(doc, arg),
                            **add_argument_kwargs(params, arg))

    for arg in required:
        parser.add_argument(arg,
                            help=get_desc(doc, arg),
                            **add_argument_kwargs(params, arg))

    if doc['summary']:
        desc = parser.description
        parser.description = '{}. {}'.format(desc, doc['summary'])

    return required, defaults


def _process_file_dir_or_glob(parser):
    """
    Process a file entry point file or directory), returns the initialized dag
    and parsed args

    Parameters
    ----------
    parser : CustomParser
        CLI arg parser
    """
    # look for env.yaml by searching in default locations
    path_to_env = default.path_to_env(
        Path(parser.parse_entry_point_value()).parent)

    if path_to_env:
        env_dict = EnvDict(path_to_env)
        _add_cli_args_from_env_dict_keys(parser, env_dict)

    args = parser.parse_args()

    if hasattr(args, 'log'):
        if args.log is not None:
            logging.basicConfig(level=args.log.upper())

    entry_point = EntryPoint(args.entry_point)

    if entry_point.type == EntryPoint.Directory:
        dag = DAGSpec.from_directory(args.entry_point).to_dag()
    elif entry_point.type == EntryPoint.Pattern:
        dag = DAGSpec.from_files(args.entry_point).to_dag()
    else:
        if path_to_env:
            # and replace keys depending on passed cli args
            replaced = _env_keys_to_override(args, parser.static_args)
            env = env_dict._replace_flatten_keys(replaced)
            dag = DAGSpec(args.entry_point, env=env).to_dag()
        else:
            dag = DAGSpec(args.entry_point).to_dag()

    return dag, args


# FIXME: I think this is always used with CustomParser, we should make it
# and instance method instead
def _custom_command(parser):
    """
    Parses an entry point, adding arguments by extracting them from the env.
    Returns a dag and the parsed args
    """
    entry_point = parser.parse_entry_point_value()
    dag, args = EntryPoint(entry_point).load(parser, sys.argv)
    return dag, args


def _flatten_dict(d, prefix=''):
    """
    Convert a nested dict: {'a': {'b': 1}} -> {'a--b': 1}
    """
    out = {}

    for k, v in d.items():
        if isinstance(v, Mapping):
            out = {**out, **_flatten_dict(v, prefix=prefix + k + '--')}
        else:
            out[prefix + k] = v

    return out
