"""
Reference material:
https://tenthousandmeters.com/blog/python-behind-the-scenes-11-how-the-python-import-system-works
"""
import sys
import warnings
from itertools import chain
import importlib
from pathlib import Path
import site

import parso

from ploomber.codediffer import _delete_python_comments
from ploomber.io import pretty_print

_SITE_PACKAGES = site.getsitepackages()


# TODO: delete this
def parent_or_child(path_to_script, origin):
    """
    Returns true if there is a parent or child relationship between the
    two arguments
    """
    try:
        Path(origin).relative_to(path_to_script)
    except ValueError:
        child = False
    else:
        child = True

    if child:
        return True

    try:
        Path(path_to_script).relative_to(origin)
    except ValueError:
        parent = False
    else:
        parent = True

    return parent


def should_track(path_to_script):
    # TODO: there might be some edge cases if running in a virtual env
    # and sys/site returns the paths to the python installation where the
    # virtual env was created from
    if Path(path_to_script).is_relative_to(sys.prefix):
        return False

    if Path(path_to_script).is_relative_to(sys.base_prefix):
        return False

    for path in _SITE_PACKAGES:
        if Path(path_to_script).is_relative_to(path):
            return False
    return True


def get_origin(dotted_path):
    """
    Gets the spec origin for the given dotted path. Returns None if cannot
    find a spec for the dotted path
    """
    try:
        # FIXME: how would this work for relative imports if the .. parts
        # does not get here?
        spec = importlib.util.find_spec(dotted_path)
    except ModuleNotFoundError:
        spec = None

    if spec:
        return Path(spec.origin), True

    # name could be an attribute, not a module. so we try to locate
    # the module instead
    name_parent = '.'.join(dotted_path.split('.')[:-1])

    try:
        spec = importlib.util.find_spec(name_parent)
    except ModuleNotFoundError:
        return None, None
    else:
        return Path(spec.origin), False


def should_track_dotted_path(path_to_script, dotted_path):
    origin, found_spec = get_origin(dotted_path)

    if found_spec:
        return should_track(origin)
    else:
        return False


def get_source_from_import(dotted_path, source_code, name_defined, base):
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

    base : str
        Source locationn (source argument), if the imported source code is not
        a child or a parent of the source, it is ignored
    """
    # if name is a symbol, return a dict with the source, if it's a module
    # return the sources for the attribtues used in source. Note that origin
    # may be None, e.g., if there is an empty package/ (no __init__.py)
    origin, is_module = get_origin(dotted_path)

    # do not obtain sources for modules that arent in the project
    if not origin or not should_track(origin):
        return {}

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

    # is a single symbol
    else:
        # TODO: maybe use the jedi library? it has a search function
        # may solve the problem with imports inside __init__.py renames
        # etc
        symbol = dotted_path.split('.')[-1]
        # TODO: only read once
        source_code_extracted = extract_symbol(origin.read_text(), symbol)
        return {dotted_path: source_code_extracted}


def extract_from_script(path_to_script):
    """
    Extract the source code for all imports in a script. Keys are dotted
    paths to the imported attributes while keys contain the source code. If
    a module is imported, only attributes used in the script are returned.

    Notes
    -----
    Star imports (from module import *) are ignored
    """
    base = Path(path_to_script).parent.resolve()
    source_code = Path(path_to_script).read_text()

    tree = parso.parse(source_code)

    specs = {}

    star_imports = []

    # this for only iters over top-level imports (?), should we ignored
    # nested ones?
    for import_ in tree.iter_imports():
        if (import_.is_star_import() and should_track_dotted_path(
                path_to_script, import_.children[1].value)):
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
                **get_source_from_import(name, source_code, name_defined, base)
            }

    if star_imports:
        warnings.warn(f'{str(path_to_script)!r} contains star imports '
                      f'({pretty_print.iterable(star_imports)}) '
                      'which prevents appropriate source code tracking.')

    return specs


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
