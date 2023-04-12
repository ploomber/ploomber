from copy import copy, deepcopy
import importlib
from pathlib import Path
from collections.abc import Mapping
from reprlib import Repr

import yaml
from jinja2 import Template, StrictUndefined

from ploomber.env import validate
from ploomber.env.expand import EnvironmentExpander, iterate_nested_dict
from ploomber.env.frozenjson import FrozenJSON
from ploomber import repo
from ploomber.util import default
from ploomber.util.validate import keys as validate_keys
from ploomber.placeholders import util
from ploomber.exceptions import ValidationError
from ploomber_core.exceptions import BaseException


def _get_import_from(raw_data, path_to_here):
    meta = raw_data.get("meta", {})

    if not isinstance(meta, Mapping):
        raise ValidationError(
            "Expected 'meta' to contain a "
            f"dictionary, but got: {meta} ({type(meta).__name__})"
        )

    validate_keys(valid={"import_from"}, passed=set(meta), name="meta")

    if "import_from" in meta:
        import_from = meta["import_from"]

        if not isinstance(import_from, str):
            raise ValidationError(
                "Expected 'import_from' to contain a "
                "string, but "
                f"got: {import_from} ({type(import_from).__name__})"
            )

        path = Path(path_to_here, import_from)

        if not path.is_file():
            raise ValidationError(
                "Expected import_from "
                f"value {import_from!r} to be a path to a file, "
                "but such file does not exist"
            )

        return EnvDict(path)._data

    return None


# TODO: custom expanders, this could be done trough another special directive
# such as _expander_class to know which class to use
class EnvDict(Mapping):
    """
    Implements the initialization functionality for Env, except it allows
    to more than one instance to exist, this is used internally to allow
    factory functions introspection without having to create an actual Env

    Parameters
    ----------
    source : dict or str
        If str, it will be interpreted as a path to a YAML file

    path_to_here : str or pathlib.Path
        Value used to expand the {{here}} placeholder. If None, it uses the
        location of the YAML spec. If initialized with a dict and None,
        the {{here}} placeholder is not available.

    defaults : dict, default=None
        Default values to use. If not None, it uses these as defaults and
        overwrites keys using values in source

    Notes
    -----
    By default, it includes the following placeholders (unless the passed
    dictionary already contains those keys): {{user}} (current user)
    {{cwd}} (working directory), {{here}} (env.yaml location, if any), {{root}}
    (project's root folder, if any)
    """

    def __init__(self, source, path_to_here=None, defaults=None):
        # if initialized from another EnvDict, copy the attributes to
        # initialize
        # this happens in the  CLI parser, which instanttiates the env
        # because it needs to create one and then replace cli args, then
        # passes this modified object to DAGSpec
        if isinstance(source, EnvDict):
            for attr in (
                "_path_to_env",
                "_preprocessed",
                "_expander",
                "_data",
                "_repr",
                "_default_keys",
                "_tags_in_raw_data",
            ):
                original = getattr(source, attr)
                setattr(self, attr, deepcopy(original))
        else:
            (
                # load data
                raw_data,
                # this will be None if source is a dict
                self._path_to_env,
            ) = load_from_source(source)

            # placeholders in user's env content
            self._tags_in_raw_data = find_tags_in_dict(raw_data)

            if defaults:
                raw_data = {**defaults, **raw_data}

            # initialize expander, which converts placeholders to their values
            # we need to pass path_to_env since the {{here}} placeholder
            # resolves to its parent
            if path_to_here is None:
                # if no pat_to_here, use path_to_end
                path_to_here = (
                    None
                    if self._path_to_env is None
                    else Path(self._path_to_env).parent
                )
            else:
                path_to_here = Path(path_to_here).resolve()

            # add default placeholders but override them if they are defined
            # in the raw data
            default = self._default_dict(path_to_here=path_to_here)
            self._default_keys = set(default) - set(raw_data)
            raw_data = {**default, **raw_data}

            # check if this one is importing
            import_from = _get_import_from(raw_data, path_to_here)

            if import_from:
                raw_data = deep_merge(import_from, raw_data)

            # check raw data is ok
            validate.raw_data_keys(raw_data)

            # expand _module special key, return its expanded value
            self._preprocessed = raw_preprocess(raw_data, self._path_to_env)

            self._expander = EnvironmentExpander(
                self._preprocessed,
                path_to_here=path_to_here,
                path_to_env=self._path_to_env,
            )
            # now expand all values
            data = self._expander.expand_raw_dictionary(raw_data)
            # this section is for config and should not be visible
            data.pop("meta", None)
            self._data = data

            self._repr = Repr()

    @classmethod
    def find(cls, source):
        """
        Find env file recursively, currently only used by the @with_env
        decorator
        """
        if not Path(source).exists():
            source_found, _ = default.find_file_recursively(source)

            if source_found is None:
                raise FileNotFoundError(
                    'Could not find file "{}" in the '
                    "current working directory nor "
                    "6 levels up".format(source)
                )
            else:
                source = source_found

        return cls(source, path_to_here=Path(source).parent)

    @property
    def default_keys(self):
        """Returns keys whose default value is used (i.e., if the user
        overrides them, they won't appear)
        """
        return self._default_keys

    @staticmethod
    def _default_dict(path_to_here):
        placeholders = {
            "user": "{{user}}",
            "cwd": "{{cwd}}",
            "now": "{{now}}",
        }

        if default.try_to_find_root_recursively(path_to_here) is not None:
            placeholders["root"] = "{{root}}"

        if path_to_here is not None:
            placeholders["here"] = "{{here}}"

        if path_to_here is not None and repo.is_repo(path_to_here):
            placeholders["git"] = "{{git}}"
            placeholders["git_hash"] = "{{git_hash}}"

        return placeholders

    @property
    def path_to_env(self):
        return self._path_to_env

    def __getattr__(self, key):
        error = AttributeError(
            "'{}' object has no attribute '{}'".format(type(self).__name__, key)
        )
        # do not look up special atttributes this way!
        if key.startswith("__") and key.endswith("__"):
            raise error

        if key in self:
            return self[key]
        else:
            raise AttributeError(
                "{} object has no atttribute '{}'".format(repr(self), key)
            )

    def __getitem__(self, key):
        try:
            return self._getitem(key)
        except KeyError as e:
            # custom error will be displayed around quotes, but it's fine.
            # this is due to the KeyError.__str__ implementation
            msg = "{} object has no key '{}'".format(repr(self), key)
            e.args = (msg,)
            raise

    def _getitem(self, key):
        if key in self._preprocessed:
            return FrozenJSON(self._preprocessed[key])
        else:
            return FrozenJSON(self._data[key])

    def __setitem__(self, key, value):
        self._data[key] = value

    def __iter__(self):
        for k in self._data:
            yield k

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        content = self._repr.repr_dict(self._data, level=2)
        return f"{type(self).__name__}({content})"

    def _replace_value(self, value, keys_all):
        """
        Replace a value in the underlying dictionary, by passing a value and
        a list of keys

        e.g. given {'a': {'b': 1}}, we can replace 1 by doing
        _replace_value(2, ['a', 'b'])
        """
        keys_to_final_dict = keys_all[:-1]
        key_to_edit = keys_all[-1]

        dict_to_edit = self._data

        for e in keys_to_final_dict:
            dict_to_edit = dict_to_edit[e]

        if key_to_edit not in dict_to_edit:
            dotted_path = ".".join(keys_all)
            raise KeyError(
                'Trying to replace key "{}" in env, '
                "but it does not exist".format(dotted_path)
            )

        dict_to_edit[key_to_edit] = self._expander.expand_raw_value(value, keys_all)

    def _inplace_replace_flatten_key(self, value, key_flatten):
        """
        Replace a value in the underlying dictionary, by passing a value and
        a list of keys

        e.g. given {'a': {'b': 1}}, we can replace 1 by doing
        _replace_flatten_keys(2, 'env__a__b'). This function is used
        internally to overrive env values when calling factories (functions
        decorated with @with_env or when doing so via the command line
        interface - ploomber build pipeline.yaml --env--a--b 2)

        Returns a copy
        """
        # convert env__a__b__c -> ['a', 'b', 'c']
        parts = key_flatten.split("__")

        if parts[0] != "env":
            raise ValueError("keys_flatten must start with env__")

        keys_all = parts[1:]
        self._replace_value(value, keys_all)

    def _replace_flatten_key(self, value, key_flatten):
        obj = copy(self)
        obj._inplace_replace_flatten_key(value, key_flatten)
        return obj

    def _inplace_replace_flatten_keys(self, to_replace):
        """Replace multiple keys at once

        Returns a copy
        """
        for key, value in to_replace.items():
            self._inplace_replace_flatten_key(value, key)

    def _replace_flatten_keys(self, to_replace):
        obj = copy(self)
        obj._inplace_replace_flatten_keys(to_replace)
        return obj

    def _render(self, raw_value):
        placeholders = util.get_tags_in_str(
            raw_value, require_runtime_placeholders=False
        )

        if not placeholders:
            return raw_value, placeholders

        undefined = placeholders - set(self)

        if undefined:
            msg = _error_message_for_undefined_list(undefined)
            raise BaseException(
                f"Error replacing placeholders:\n{msg}\n\nLoaded env: " f"{self!r}"
            )

        # render using self._data since self contains FrozenJSON objects
        value = Template(raw_value, undefined=StrictUndefined).render(**self._data)

        return value, placeholders

    def get_unused_placeholders(self):
        """
        Returns unused placeholders, can be used to warn users. Excludes
        default values such as {{now}} and {{user}}
        """
        return set(self) - self.default_keys - self._tags_in_raw_data


def _error_message_for_undefined_list(undefined):
    return "\n".join(_error_message_for_undefined(u) for u in undefined)


def _error_message_for_undefined(u):
    git_ = "Ensure git is installed and git repository exists"
    reasons = {
        "git": git_,
        "git_hash": git_,
        "here": "Ensure the spec was initialized from a file",
        "root": (
            "Ensure a pipeline.yaml or setup.py exist in the "
            "current directory or a parent directory"
        ),
    }
    howto = reasons.get(u, "Ensure the placeholder is defined in the env")

    return "  * {{" + u + "}}: " + howto


def load_from_source(source):
    """
    Loads from a dictionary or a YAML and applies preprocesssing to the
    dictionary

    Returns
    -------
    dict
        Raw dictionary
    pathlib.Path
        Path to the loaded file, None if source is a dict
    str
        Name, if loaded from a YAML file with the env.{name}.yaml format,
        None if another format or if source is a dict
    """
    if isinstance(source, Mapping):
        # dictiionary, path
        return source, None

    with open(str(source)) as f:
        try:
            raw = yaml.load(f, Loader=yaml.SafeLoader)
        except Exception as e:
            raise type(e)(
                "yaml.load failed to parse your YAML file "
                "fix syntax errors and try again"
            ) from e
        finally:
            # yaml.load returns None for empty files and str if file just
            # contains a string - those aren't valid for our use case, raise
            # an error
            if not isinstance(raw, Mapping):
                raise ValueError(
                    "Expected object loaded from '{}' to be "
                    "a dict but got '{}' instead, "
                    "verify the content".format(source, type(raw).__name__)
                )

    path = Path(source).resolve()

    return raw, path


def raw_preprocess(raw, path_to_raw):
    """
    Preprocess a raw dictionary. If a '_module' key exists, it
    will be expanded: first, try to locate a module with that name and resolve
    to their location (root __init__.py parent), if no module is found,
    interpret as a path to the project's root folder, checks that the folder
    actually exists. '{{here}}' is also allowed, which resolves to the
    path_to_raw, raises Exception if path_to_raw is None

    Returns
    -------
    preprocessed : dict
        Dict with preprocessed keys (empty dictionary it no special
        keys exist in raw)

    Parameters
    ----------
    raw : dict
        Raw data dictionary
    path_to_raw : str
        Path to file where dict was read from, it read from a dict, pass None
    """
    module = raw.get("_module")
    preprocessed = {}

    if module:
        if raw["_module"] == "{{here}}":
            if path_to_raw is not None:
                preprocessed["_module"] = path_to_raw.parent
            else:
                raise ValueError(
                    "_module cannot be {{here}} if " "not loaded from a file"
                )
        else:
            # check if it's a filesystem path
            as_path = Path(module)

            if as_path.exists():
                if as_path.is_file():
                    raise ValueError(
                        'Could not resolve _module "{}", '
                        "expected a module or a directory but got a "
                        "file".format(module)
                    )
                else:
                    path_to_module = as_path

            # must be a dotted path
            else:
                module_spec = importlib.util.find_spec(module)

                # package does not exist
                if module_spec is None:
                    raise ValueError(
                        'Could not resolve _module "{}", '
                        "it is not a valid module "
                        "nor a directory".format(module)
                    )
                else:
                    path_to_module = Path(module_spec.origin).parent

            preprocessed["_module"] = path_to_module

    return preprocessed


def find_tags_in_dict(d):
    tags = set()

    for k in iterate_nested_dict(d):
        tags = tags | util.get_tags_in_str(k)

    return tags


def deep_merge(a: dict, b: dict) -> dict:
    result = deepcopy(a)
    for bk, bv in b.items():
        av = result.get(bk)
        if isinstance(av, dict) and isinstance(bv, dict):
            result[bk] = deep_merge(av, bv)
        else:
            result[bk] = deepcopy(bv)
    return result
