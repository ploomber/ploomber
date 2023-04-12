from pathlib import Path
import os
import logging
import sys
import importlib
import inspect
import argparse
from collections.abc import Mapping
import warnings

try:
    import importlib.resources as importlib_resources
except ImportError:  # pragma: no cover
    # backported
    import importlib_resources

from ploomber.spec.dagspec import DAGSpec
from ploomber.env.envdict import EnvDict
from ploomber.util.dotted_path import load_dotted_path
from ploomber.util import default
from ploomber.entrypoint import EntryPoint


def process_arg(s):
    clean = None

    if s.startswith("--"):
        clean = s[2:]
    elif s.startswith("-"):
        clean = s[1:]
    else:
        clean = s

    return clean.replace("-", "_")


class CustomMutuallyExclusiveGroup(argparse._MutuallyExclusiveGroup):
    """
    A subclass of argparse._MutuallyExclusiveGroup that determines whether
    the added args should go into the static of dynamic API
    """

    def add_argument(self, *args, **kwargs):
        if not self._container.finished_static_api:
            if not self._container.in_context and self._container.finished_init:
                raise RuntimeError(
                    "Cannot add arguments until the static " "API has been declared"
                )
            else:
                # running inside the context manager
                self._container.static_args.extend([process_arg(arg) for arg in args])

        # outside context manager
        return super().add_argument(*args, **kwargs)


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
        self.DEFAULT_ENTRY_POINT = default.try_to_find_entry_point()

        self.static_args = []
        self.finished_static_api = False
        self.in_context = False
        self.finished_init = False
        super().__init__(*args, **kwargs)

        self.add_argument(
            "--log",
            "-l",
            help="Enables logging to stdout at the " "specified level",
            default=None,
        )

        self.add_argument(
            "--log-file", "-F", help="Enables logging to the given file", default=None
        )

        if self.DEFAULT_ENTRY_POINT:
            entry_point_help = "Entry point, defaults " f"to {self.DEFAULT_ENTRY_POINT}"
            if os.environ.get("ENTRY_POINT"):
                entry_point_help += " (ENTRY_POINT env var)"
        else:
            entry_point_help = "Entry point"

        self.add_argument(
            "--entry-point",
            "-e",
            help=entry_point_help,
            default=self.DEFAULT_ENTRY_POINT,
        )

        self.finished_init = True

    def parse_entry_point_value(self):
        """
        Returns the entry_point value pased without calling parse_args(),
        this is required to find env params to show, if we call parse_args()
        the CLI stops there and shows available params
        """
        index = None

        try:
            index = sys.argv.index("--entry-point")
        except ValueError:
            pass

        try:
            index = sys.argv.index("-e")
        except ValueError:
            pass

        # no --entry-point/-e arg passed, use default
        if index is None:
            if self.DEFAULT_ENTRY_POINT is None:
                self.error(
                    "Unable to find a pipeline. "
                    "Use --entry-point/-e to pass a "
                    "entry point's location or "
                    "place it in a standard location.\n\n"
                    "Otherwise check if your pipeline have "
                    ".yml as extension, "
                    "change it to .yaml instead.\n\n"
                    "Need help? https://ploomber.io/community"
                )

            return self.DEFAULT_ENTRY_POINT
        else:
            try:
                return sys.argv[index + 1]
            except IndexError:
                pass

            # replicate the original message emitted by argparse
            action = self._option_string_actions["-e"]
            options = "/".join(action.option_strings)
            self.error(f"argument {options}: expected one argument")

    def add_argument(self, *args, **kwargs):
        """
        Add a CLI argument. If called after the context manager, it is
        considered part of the dynamic API, if called within the context
        manager, the arg is considered part of the static API. If it's
        called outside a context manager, and no static API has been set,
        it raises an error
        """
        if not self.finished_static_api:
            if not self.in_context and self.finished_init:
                raise RuntimeError(
                    "Cannot add arguments until the static " "API has been declared"
                )
            else:
                # running inside the context manager
                self.static_args.extend([process_arg(arg) for arg in args])

        # outside context manager
        return super().add_argument(*args, **kwargs)

    def add_mutually_exclusive_group(self, **kwargs):
        """
        Add a mutually exclusive group. It returns a custom class that
        correctly stores the arguments in the static or dynamic API
        """
        group = CustomMutuallyExclusiveGroup(self, **kwargs)
        self._mutually_exclusive_groups.append(group)
        return group

    def __enter__(self):
        self.in_context = True
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.in_context = False
        self.finished_static_api = True

    def process_factory_dotted_path(self, dotted_path):
        """Parse a factory entry point, returns initialized dag and parsed args"""
        entry = load_dotted_path(str(dotted_path), raise_=True)

        # add args using the function's signature
        required, _ = _add_args_from_callable(self, entry)

        # if entry point was decorated with @with_env, add arguments
        # to replace declared variables in env.yaml
        if hasattr(entry, "_env_dict"):
            _add_cli_args_from_env_dict_keys(self, entry._env_dict)

        args = self.parse_args()

        _configure_logger(args)

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

    def load_from_entry_point_arg(self):
        """
        Parses an entry point, adding arguments by extracting them from
        the env.

        Returns a dag and the parsed args
        """
        entry_point = EntryPoint(self.parse_entry_point_value())
        dag, args = load_dag_from_entry_point_and_parser(entry_point, self, sys.argv)
        return dag, args


def _path_for_module_path(module_path):
    mod_name, path_part = module_path.split("::")

    # TODO: check it's only two parts after splitting
    with importlib_resources.path(mod_name, path_part) as p:
        path = str(p)

    return path


def _first_non_empty_line(doc):
    for line in doc.split("\n"):
        if line:
            return line.strip()


def _parse_doc(callable_):
    """
    Convert numpydoc docstring to a list of dictionaries
    """
    doc = callable_.__doc__

    # no docstring
    if doc is None:
        return {"params": {}, "summary": None}

    # try to import numpydoc, if can't find it, just returnt the first line
    try:
        docscrape = importlib.import_module("numpydoc.docscrape")
    except ModuleNotFoundError:
        return {"params": {}, "summary": _first_non_empty_line(doc)}

    doc_parsed = docscrape.NumpyDocString(doc)

    parameters = {
        p.name: {"desc": " ".join(p.desc), "type": p.type}
        for p in doc_parsed["Parameters"]
    }

    # docscrape returns one element per line
    summary = "Docstring: {}".format("\n".join(doc_parsed["Summary"]))

    return {"params": parameters, "summary": summary}


def _env_keys_to_override(args, static_args):
    """
    Returns a dictionary with all extra cli parameters passed, all these must
    be parameters that part of the env or params (with no defaults) if
    entry point is a factory function
    """
    return {
        name: getattr(args, name)
        for name in dir(args)
        if not name.startswith("_")
        if getattr(args, name) is not None
        if name not in static_args
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
            parser.add_argument("--env--" + arg, help="Default: {}".format(val))


def _parse_signature_from_callable(callable_):
    """
    Parse a callable signature, return a dictionary with
    {param_key: default_value} and a list of required parameters
    """
    sig = inspect.signature(callable_)

    required = [k for k, v in sig.parameters.items() if v.default == inspect._empty]

    defaults = {
        k: v.default for k, v in sig.parameters.items() if v.default != inspect._empty
    }

    return required, defaults, sig.parameters


def get_desc(doc, arg):
    arg_data = doc["params"].get(arg)
    return None if arg_data is None else arg_data["desc"]


def add_argument_kwargs(params, arg):
    """
    Build kwargs for parser.add_argument function
    """
    valid_hints = [int, float, str, bool]

    fn_default = params[arg].default
    fn_annotation = params[arg].annotation

    # special case, bool with default value becomes a flag
    if fn_annotation is bool and fn_default is not inspect._empty:
        kwargs = {"action": "store_true" if not fn_default else "store_false"}
    elif fn_annotation in valid_hints:
        kwargs = {"type": fn_annotation, "default": fn_default}
    else:
        kwargs = {"default": fn_default}

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
        conflict = False

        try:
            parser.add_argument(
                "--" + arg, help=get_desc(doc, arg), **add_argument_kwargs(params, arg)
            )
        except argparse.ArgumentError as e:
            conflict = e

        if conflict:
            if "conflicting option string" in conflict.message:
                raise ValueError(
                    f"The signature from {callable_.__name__!r} "
                    "conflicts with existing arguments in the command-line "
                    "interface, please rename the following "
                    f"argument: {arg!r}"
                )
            else:
                raise conflict

    for arg in required:
        parser.add_argument(
            arg, help=get_desc(doc, arg), **add_argument_kwargs(params, arg)
        )

    if doc["summary"]:
        desc = parser.description
        parser.description = "{}. {}".format(desc, doc["summary"])

    return required, defaults


def _process_file_dir_or_glob(parser, dagspec_arg=None):
    """
    Process a file entry point file, directory or glob-like pattern,
    the initialized dag and parsed args

    Parameters
    ----------
    parser : CustomParser
        CLI arg parser
    """
    # NOTE: we must use parser.parse_entry_point_value() instead or
    # args.parse_args because calling the latter wont allow us to add more
    # cli parameters, but we want that to expose parms from env
    entry_point_value = dagspec_arg or parser.parse_entry_point_value()
    entry = EntryPoint(entry_point_value)

    if entry.type in {EntryPoint.Directory, EntryPoint.Pattern}:
        # pipelines initialized from directories or patterns cannot be
        # parametrized
        path_to_env = None
    # file
    else:
        path_to_env = default.path_to_env_from_spec(entry_point_value)

    if path_to_env:
        env_dict = EnvDict(
            path_to_env,
            path_to_here=Path(entry_point_value).parent
            if entry.type == EntryPoint.File
            else None,
        )
        _add_cli_args_from_env_dict_keys(parser, env_dict)

    args = parser.parse_args()
    dagspec_arg = dagspec_arg or args.entry_point

    _configure_logger(args)

    entry_point = EntryPoint(dagspec_arg)

    # directory
    if entry_point.type == EntryPoint.Directory:
        dag = DAGSpec.from_directory(dagspec_arg).to_dag()
    # pattern
    elif entry_point.type == EntryPoint.Pattern:
        dag = DAGSpec.from_files(dagspec_arg).to_dag()
    # file
    else:
        if path_to_env:
            # and replace keys depending on passed cli args
            replaced = _env_keys_to_override(args, parser.static_args)
            env = env_dict._replace_flatten_keys(replaced)
            dag = DAGSpec(dagspec_arg, env=env).to_dag()
        else:
            dag = DAGSpec(dagspec_arg).to_dag()

    return dag, args


def load_dag_from_entry_point_and_parser(entry_point, parser, argv):
    """Load DAG from entry point

    Parameters
    ----------
    parser : CustomParser
        The cli parser object

    argv : list
        Command line arguments
    """
    help_cmd = "--help" in argv or "-h" in argv

    # if the file does not exist but the value has sufix yaml/yml, show a
    # warning because the last thing to try is to interpret it as a dotted
    # path and that's probably not what the user wants
    if not entry_point.exists() and entry_point.suffix in {".yaml", ".yml"}:
        warnings.warn(
            'Entry point value "{}" has extension "{}", which '
            "suggests a spec file, but the file doesn't "
            "exist".format(entry_point, entry_point.suffix)
        )

    # even if the entry file is not a file nor a valid module, show the
    # help menu, but show a warning
    if help_cmd and not entry_point.exists():
        warnings.warn(
            'Failed to load entry point "{}". It is not a file '
            "nor a valid dotted path".format(entry_point)
        )

        args = parser.parse_args()

    # at this point there are two remaining cases:
    # no help command (entry point may or may not exist),:
    #   we attempt to run the command
    # help command and exists:
    #   we just parse parameters to display them in the help menu
    elif entry_point.type == EntryPoint.DottedPath:
        dag, args = parser.process_factory_dotted_path(entry_point)
    elif entry_point.type == EntryPoint.ModulePath:
        dag, args = _process_file_dir_or_glob(
            parser, dagspec_arg=_path_for_module_path(entry_point.value)
        )
    else:
        # process file, directory or glob pattern
        dag, args = _process_file_dir_or_glob(parser)

    return dag, args


def _flatten_dict(d, prefix=""):
    """
    Convert a nested dict: {'a': {'b': 1}} -> {'a--b': 1}
    """
    out = {}

    for k, v in d.items():
        if isinstance(v, Mapping):
            out = {**out, **_flatten_dict(v, prefix=prefix + k + "--")}
        else:
            out[prefix + k] = v

    return out


def _configure_logger(args):
    """Configure logger if user passed --log/--log-file args"""
    if hasattr(args, "log"):
        if args.log is not None:
            logging.basicConfig(level=args.log.upper())

    if hasattr(args, "log_file"):
        if args.log_file is not None:
            file_handler = logging.FileHandler(args.log_file)
            logging.getLogger().addHandler(file_handler)
