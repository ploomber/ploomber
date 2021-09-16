"""
Reference material:
https://tenthousandmeters.com/blog/python-behind-the-scenes-11-how-the-python-import-system-works
"""
from copy import copy
import sys
import warnings
from itertools import chain
import importlib
from pathlib import Path
import site
import inspect

import parso

from ploomber.codediffer import _delete_python_comments
from ploomber.io import pretty_print

_SITE_PACKAGES = site.getsitepackages()


def should_track_origin(path_to_origin):
    """
    Determines if an origin (a path in most cases) should be tracked for source
    code changes. Excludes built-in and site-packages

    Returns
    -------
    bool
        True if the file should be tracked
    """
    # this happens on Windows when calling importlib.util.find_spec(name)
    # where name is a built-in module
    if str(path_to_origin) == 'built-in':
        return False

    # TODO: there might be some edge cases if running in a virtual env
    # and sys/site returns the paths to the python installation where the
    # virtual env was created from
    if _path_is_relative_to(path_to_origin, sys.prefix):
        return False

    if _path_is_relative_to(path_to_origin, sys.base_prefix):
        return False

    for path in _SITE_PACKAGES:
        if _path_is_relative_to(path_to_origin, path):
            return False

    return True


def should_track_dotted_path(dotted_path):
    """Determines if a dotted_path should be tracked
    """
    origin, _ = get_dotted_path_origin(dotted_path)
    return False if not origin else should_track_origin(origin)


def get_dotted_path_origin(dotted_path):
    """
    Returns the path to the root file of a given dotted_path and if the
    dotted_path is a module (a .py file) or not
    """
    try:
        # FIXME: how would this work for relative imports if the .. parts
        # does not get here?
        spec = importlib.util.find_spec(dotted_path)
    # NOTE: python 3.6 raises AttributeError
    # https://bugs.python.org/issue30436
    except (ModuleNotFoundError, AttributeError):
        spec = None

    if spec:
        return Path(spec.origin), True

    # name could be an attribute (e.g. a function), not a module (a file). So
    # we try to locate the module instead
    name_parent = '.'.join(dotted_path.split('.')[:-1])

    try:
        spec = importlib.util.find_spec(name_parent)
    except (ModuleNotFoundError, AttributeError):
        return None, None
    else:
        return Path(spec.origin), False


def get_source_from_import(dotted_path, source_code, name_defined):
    """
    Get source code for the given dotted path. Returns a dictionary with a
    single key-value pair if the dotted path is an module attribute, if it's
    a module, it returns one key-value pair for each attribute accessed in the
    source code.

    Parameters
    ----------
    dotted_path : str
        Dotted path with the module/attribute location. e.g., module.sub_module
        or module.sub_module.attribute

    source_code : str
        The source code where the import statement used to generatet the dotted
        path exists

    name_defined : str
        The name defined my the import statement. e.g.,
        "import my_module.sub_module as some_name" imports sub_module but
        defines it in the some_name variable. This is used to look for
        references in the code and return the source for the requested
        attributes
    """
    # if name is a symbol, return a dict with the source, if it's a module
    # return the sources for the attribtues used in source. Note that origin
    # may be None, e.g., if there is an empty package/ (no __init__.py)
    origin, is_module = get_dotted_path_origin(dotted_path)

    # do not obtain sources for modules that arent in the project
    if not origin or not should_track_origin(origin):
        return {}

    # it's a module (user may access only a few attributes)
    if is_module:
        # everything except the last element
        accessed_attributes = extract_attribute_access(source_code,
                                                       name_defined)

        # TODO: only read origin once
        out = {
            f'{dotted_path}.{attr}': extract_symbol(origin.read_text(), attr)
            for attr in accessed_attributes
        }

        # remove symbols that do not exist
        return {k: v for k, v in out.items() if v is not None}

    # it's a single symbol (user imported the function/class)
    else:
        # TODO: maybe use the jedi library? it has a search function
        # may solve the problem with imports inside __init__.py renames
        # etc
        symbol = dotted_path.split('.')[-1]
        return _get_source_from_accessed_symbol(
            dotted_path,
            source_code,
            origin.read_text(),
            symbol,
        )


def _get_source_from_accessed_symbol(dotted_path, source_code, origin, symbol):
    name_accessed = did_access_name(source_code, symbol)

    if name_accessed:
        # TODO: only read once
        source_code_extracted = extract_symbol(origin, symbol)
        return {dotted_path: source_code_extracted}
    else:
        return {}


def extract_from_script(path_to_script):
    """
    Extract source code from all imported and used objects in a script. Keys
    are dotted paths to the imported attributes while keys contain the source
    code. Unused objects (even if imported) are ignored.

    Notes
    -----
    Star imports (from module import *) are ignored
    """
    source = Path(path_to_script).read_text()
    return _extract_imported_objects_from_source(source, path_to_script)


def extract_from_callable(callable_):
    """
    Extract source code from all objects used inside a function's body. These
    include imported objects and objects defined in the same module as the
    callable.
    """
    # NOTE: we can use the inspect module here since by the time we call
    # this, the function has already been imported and executed
    source = inspect.getsource(callable_)
    path_to_source = inspect.getsourcefile(callable_)
    imports = Path(path_to_source).read_text()

    # this returns symbols used through imports
    from_imports = _extract_imported_objects_from_source(
        source, path_to_source, imports)
    # and this from symbols defined in the same file
    local = _extract_accessed_objects_from_callable(callable_)

    # NOTE: what if there are duplicates?
    # import x
    # def x():
    #     pass
    # maybe a warning?
    extracted = {**from_imports, **local}
    final = copy(extracted)

    for dotted_path in extracted.keys():
        new = extract_from_callable(_load_dotted_path(dotted_path))
        final.update(new)

    return final


def _extract_accessed_objects_from_callable(callable_):
    """Extract source code from all locally defined objects used by a callable
    """
    # is there any differente between this and inspect.getmodule?
    mod_name = callable_.__module__

    tree = parso.parse(Path(inspect.getsourcefile(callable_)).read_text())

    defined = {}

    for def_ in chain(tree.iter_funcdefs(), tree.iter_classdefs()):
        if def_.name.value != callable_.__name__:
            defined[def_.name.value] = def_.get_code().strip()

    fn_source = inspect.getsource(callable_)

    accessed = get_accessed_names(fn_source, defined)

    return {f'{mod_name}.{k}': v for k, v in defined.items() if k in accessed}


def _extract_imported_objects_from_source(source_code,
                                          path_to_source,
                                          imports=None):
    """Extract source code from all imported objects used in a code string

    Parameters
    ----------
    source_code : str
        The code string used to check object access

    path_to_source : str or pathlib.Path
        Path to source_code. Only used to warn the user if source_code contains
        star imports

    imports : str, default=None
        A code string with imports. If None, imports in source_code are used
    """
    tree = parso.parse(imports or source_code)

    specs = {}
    star_imports = []

    # this for only iters over top-level imports (?), should we ignored
    # nested ones?

    for import_ in tree.iter_imports():
        if (import_.is_star_import()
                and should_track_dotted_path(import_.children[1].value)):
            star_imports.append(import_.get_code().strip())

        # iterate over paths. e.g., from mod import a, b
        # iterates over a and b
        for paths, name_defined in zip(import_.get_paths(),
                                       import_.get_defined_names()):
            name = '.'.join([name.value for name in paths])

            # if import_name: import a.b, look for attributes of a.b
            # (e.g.,a.b.c)
            # if import_from: from a import b, look for attributes of b
            # (e.g., b.c)

            # use the keyword next to import if doing (import X) and we are
            # not using the as keyword
            if (import_.type == 'import_name' and 'dotted_as_name'
                    not in [c.type for c in import_.children]):
                name_defined = name
            else:
                name_defined = name_defined.value

            specs = {
                **specs,
                **get_source_from_import(name, source_code, name_defined)
            }

    if star_imports:
        warnings.warn(f'{str(path_to_source)!r} contains star imports '
                      f'({pretty_print.iterable(star_imports)}) '
                      'which prevents appropriate source code tracking.')

    return specs


def _load_dotted_path(dotted_path):
    tokens = dotted_path.split('.')
    mod, attr = '.'.join(tokens[:-1]), tokens[-1]
    mod_obj = importlib.import_module(mod)
    return getattr(mod_obj, attr)


def did_access_name(code, name):
    """Check if a defined name is accessed in the code string
    """
    # delete comments otherwise the leaf.parent.get_code() will fail
    m = parso.parse(_delete_python_comments(code))

    leaf = m.get_first_leaf()

    while leaf is not None:
        if leaf.get_code().strip() == name:
            return True

        # can i make this faster? is there a way to get the next leaf
        # of certain type?
        leaf = leaf.get_next_leaf()

    return False


def get_accessed_names(code, names):
    """Returns a subset of accessed names in the code string
    """
    remaining = set(names)
    accessed = []

    # delete comments otherwise the leaf.parent.get_code() will fail
    m = parso.parse(_delete_python_comments(code))

    leaf = m.get_first_leaf()

    while leaf is not None and len(remaining):
        name_found = leaf.get_code().strip()

        if name_found in remaining:
            remaining.remove(name_found)
            accessed.append(name_found)

        # can i make this faster? is there a way to get the next leaf
        # of certain type?
        leaf = leaf.get_next_leaf()

    return accessed


def extract_attribute_access(code, name):
    """
    Extracts all attributes accessed with a given name. e.g., if name = 'obj',
    then this procedure returns all strings with the 'obj.{something}' form,
    this includes things like: obj.something, obj.something[1],
    obj.something(1)

    Parameters
    ----------
    code : str
        The code to analyze

    name : str
        The variable to check
    """
    # delete comments otherwise the leaf.parent.get_code() will fail
    m = parso.parse(_delete_python_comments(code))

    attributes = []

    n_tokens = len(name.split('.'))
    leaf = m.get_first_leaf()

    while leaf is not None:

        # get the full matched dotte path (e.g., a.b.c.d())
        matched_dotted_path = leaf.parent.get_code().strip()

        # newline and endmarker also have the dotted path as parent so we
        # ignore them. make sure the matched dotted path starts with the name
        # we want to check
        if (leaf.type not in {'newline', 'endmarker'}
                and matched_dotted_path.startswith(name)):

            # get all the elements in the dotted path
            children = leaf.parent.children
            children_code = [c.get_code() for c in children]

            # get tokens that start with "." (to ignore function calls or
            # getitem)
            last = '.'.join([
                token.replace('.', '') for token in children_code[n_tokens:]
                if token[0] == '.'
            ])

            if last:
                attributes.append(last)

        # can i make this faster? is there a way to get the next leaf
        # of certain type?
        leaf = leaf.get_next_leaf()

    return attributes


def extract_symbol(code, name):
    """Get source code for symbol with a given name

    Parameters
    ----------
    code : str
        Code to analyze

    name : str
        Symbol name
    """
    m = parso.parse(code)

    for node in chain(m.iter_funcdefs(), m.iter_classdefs()):
        if node.name.value == name:
            return node.get_code().strip()


def _path_is_relative_to(path, other):
    # for backwards compatibility (Python < 3.9)
    try:
        Path(path).relative_to(other)
        return True
    except ValueError:
        return False
