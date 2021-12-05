import os
import re
import ast
import pydoc
import getpass
from copy import deepcopy, copy
from collections.abc import Mapping, Iterable
from pathlib import Path
from functools import reduce
import datetime

from jinja2 import Template, StrictUndefined

from ploomber.placeholders import util
from ploomber import repo
from ploomber.util import default


def expand_raw_dictionary_and_extract_tags(raw, mapping):
    data = deepcopy(raw)

    placeholders_all = []

    for (d, current_key, current_val, _) in iterate_nested_dict(data):
        d[current_key], placeholders = expand_if_needed(current_val, mapping)
        placeholders_all.extend(placeholders)

    return data, set(placeholders_all)


def expand_raw_dictionary(raw, mapping):
    """
    Expands a dictionary where some values are {{tags}} using their values
    in a mapping
    """
    return expand_raw_dictionary_and_extract_tags(raw, mapping)[0]


def expand_raw_dictionaries_and_extract_tags(raw, mapping):
    """
    Expands a list of dictionaries
    """
    expanded, tags = list(
        zip(*[
            expand_raw_dictionary_and_extract_tags(element, mapping)
            for element in raw
        ]))

    tags_unique = set(reduce(lambda x, y: x | y, tags))

    return expanded, tags_unique


def expand_if_needed(raw_value, mapping):
    placeholders = util.get_tags_in_str(raw_value)

    if not placeholders:
        value = raw_value
    else:
        try:
            value = Template(raw_value,
                             undefined=StrictUndefined).render(**mapping)
        except Exception as e:
            exception = e
        else:
            exception = None

        if exception:
            raise KeyError(
                f'Error replacing placeholder: {exception}. Loaded env: '
                f'{mapping!r}')

    return cast_if_possible(value), placeholders


def cast_if_possible(value):
    """
    Reference to env in specs must be strings, but we would like the rendered
    value to still have the appropriate type
    """
    if isinstance(value, str):
        value_lower = value.lower()

        if value_lower == 'false':
            return False
        elif value_lower == 'true':
            return True
        elif value_lower in {'none', 'null'}:
            return None

        try:
            return ast.literal_eval(value)
        except Exception:
            pass

    return value


class EnvironmentExpander:
    """
    Convert values in the raw dictionary by expanding tags such as {{git}},
    {{version}} or {{here}}. See `expand_raw_value` for more details

    Parameters
    ----------
    preprocessed : dict
        Preprocessed env dictionary

    path_to_here : str
        Path to env.yaml, used to expand {{here}}

    version_requires_import : bool, default=False
        Whether determining package version requires import or not. If False,
        the root ``__init__.py`` file in the module must have
        a ``__version__ = 'LITERAL'`` variable. Literal is extracted.
    """
    def __init__(self,
                 preprocessed,
                 path_to_here=None,
                 version_requires_import=False):
        self._preprocessed = preprocessed

        # {{here}} resolves to this value
        self._path_to_here = (None if path_to_here is None else str(
            Path(path_to_here).resolve()))
        # we compute every placeholder's value so we only do it once
        self._placeholders = {}

        self._version_requires_import = version_requires_import

    def expand_raw_dictionary(self, raw):
        data = deepcopy(raw)

        for (d, current_key, current_val,
             parent_keys) in iterate_nested_dict(data):
            d[current_key] = self.expand_raw_value(current_val, parent_keys)

        return data

    def expand_raw_value(self, raw_value, parents):
        """
        Expand a string with placeholders

        Parameters
        ----------
        raw_value : str
            The original value to expand
        parents : list
            The list of parents to get to this value in the dictionary

        Notes
        -----
        If for a given raw_value, the first parent is 'path', expanded value
        is casted to pathlib.Path object and .expanduser() is called,
        furthermore, if raw_value ends with '/', a directory is created if
        it does not currently exist
        """
        placeholders = util.get_tags_in_str(raw_value)

        if not placeholders:
            value = raw_value
        else:
            # get all required placeholders
            params = {k: self.load_placeholder(k) for k in placeholders}
            value = Template(raw_value).render(**params)

        if parents:
            if parents[0] == 'path':

                # value is a str (since it was loaded from a yaml file),
                # if it has an explicit trailing slash, interpret it as
                # a directory and create it, we have to do it at this point,
                # because once we cast to Path, we lose the trailing slash
                if value.endswith('/'):
                    self._try_create_dir(value)

                return Path(value).expanduser()
            else:
                return cast_if_possible(value)

    def _try_create_dir(self, value):
        # make sure to expand user to avoid creating a "~" folder
        path = Path(value).expanduser()

        if not path.exists():
            path.mkdir(parents=True)

    def load_placeholder(self, key):
        if key not in self._placeholders:
            if hasattr(self, 'get_' + key):
                self._placeholders[key] = getattr(self, 'get_' + key)()
            else:
                raise RuntimeError('Unknown placeholder "{}"'.format(key))

        return self._placeholders[key]

    def _get_version_importing(self):
        module_path = self._preprocessed.get('_module')

        if not module_path:
            raise KeyError('_module key is required to use version '
                           'placeholder')

        # is this ok to do? /path/to/{module_name}
        module_name = str(Path(module_path).name)
        module = pydoc.locate(module_name)

        if module is None:
            raise ImportError(
                'Unabe to import module with name "{}"'.format(module_name))

        if hasattr(module, '__version__'):
            return module.__version__
        else:
            raise RuntimeError('Module "{}" does not have a __version__ '
                               'attribute '.format(module))

    def _get_version_without_importing(self):
        if '_module' not in self._preprocessed:
            raise KeyError('_module key is required to use version '
                           'placeholder')

        content = (self._preprocessed['_module'] / '__init__.py').read_text()

        version_re = re.compile(r'__version__\s+=\s+(.*)')

        version = str(ast.literal_eval(version_re.search(content).group(1)))
        return version

    def get_version(self):
        if self._version_requires_import:
            return self._get_version_importing()
        else:
            return self._get_version_without_importing()

    def get_user(self):
        return getpass.getuser()

    def get_cwd(self):
        return str(Path(os.getcwd()).resolve())

    def get_root(self):
        root = default.try_to_find_root_recursively(
            starting_dir=self._path_to_here)

        if root is None:
            raise ValueError('Failed to expand {{root}}, could not '
                             'find a setup.py in a parent folder')

        return root

    def get_here(self):
        if self._path_to_here:
            return self._path_to_here
        else:
            raise RuntimeError('here placeholder is only available '
                               'when env was initialized from a file or '
                               'when directly passing path to use')

    def get_git(self):
        module = self._preprocessed.get('_module')

        if not module:
            raise KeyError('_module key is required to use git placeholder')

        return repo.get_git_info(module)['git_location']

    def get_now(self):
        """Returns current timestamp in ISO 8601 format
        """
        return datetime.datetime.now().isoformat()


def iterate_nested_dict(d):
    """
    Iterate over all values (possibly nested) in a dictionary

    Yields: dict holding the value, current key, current value, list of keys
    to get to this value
    """
    for k, v in d.items():
        for i in _iterate(d, k, v, preffix=[k]):
            yield i


def _iterate(parent, key, value, preffix):
    if isinstance(value, Mapping):
        for k, v in value.items():
            preffix_new = copy(preffix)
            preffix_new.append(k)
            for i in _iterate(value, k, v, preffix_new):
                yield i
    elif isinstance(value, Iterable) and not isinstance(value, str):
        for idx, some_val in enumerate(value):
            preffix_new = copy(preffix)
            preffix_new.append(idx)
            for i in _iterate(value, idx, some_val, preffix_new):
                yield i
    else:
        yield parent, key, value, preffix
