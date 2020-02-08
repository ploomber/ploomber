"""
This module provides subclasses of jinja2 for better integration with SQL
"""
from collections.abc import Iterable
import pydoc
from pathlib import Path
from ploomber.templates.Placeholder import Placeholder

from jinja2 import Environment, FileSystemLoader, StrictUndefined


def _is_iterable(o):
    return isinstance(o, Iterable) and not isinstance(o, str)


def _is_iterable_w_types(o, types):
    for element in o:
        if not isinstance(o, types):
            return False
    return True


class SQLStore:
    """
    Utility class for loading SQL files from a folder, supports parametrized
    SQL templates (jinja2)

    Parameters
    ----------
    path : str, pathlib.Path, optional
        Path (or list of) to load files from. Required if module is None

    module : str or module, optional
        Module name as dotted string or module object. Preprends to path
        parameter

    Examples
    --------
    >>> from tax_estimator.sql import SQLStore
    >>> from ploomber import Env
    >>> env = Env()
    >>> path = env.path.home / 'load' / 'sql'
    >>> sqlstore = SQLStore(path)
    >>> sqlstore['template.sql']
    >>> sqlstore['template.sql'].path
    """

    def __init__(self, path=None, module=None):
        if path is None and module is None:
            raise TypeError('Path cannot be None if module is None')

        # validate path
        if _is_iterable(path):
            if _is_iterable_w_types(path, (str, Path)):
                types_found = set(type(element) for element in path)
                raise TypeError('If passing an iterable, path must consist '
                                'of str and pathlib.Path objects only, got '
                                '{}'.format(types_found))

            path = [str(element) for element in path]
        elif isinstance(path, Path):
            path = str(path)

        if isinstance(module, str):
            module = pydoc.locate(module)

            if module is None:
                raise ValueError('Could not locate module "{}"'.format(module))

        # for module objects
        if hasattr(module, '__file__'):
            module_path = str(Path(module.__file__).parent.absolute())

        elif module is None:
            module_path = ''
        else:
            raise ValueError('Could not find module path, pass a string or a '
                             'module object')

        if _is_iterable(path):
            path_full = [str(Path(module_path, e)) for e in path]
        else:
            # if path is None, do not append anything
            path_full = str(Path(module_path, path or ''))

        # NOTE: we do not use jinja2.PackageLoader since it does not provide
        # the abilty to pass a list of paths
        self.env = Environment(
            loader=FileSystemLoader(path_full),
            # this will cause jinja2 to raise an exception if a variable
            # declared in the template is not passed in the render parameters
            undefined=StrictUndefined)

    def __getitem__(self, key):
        return self.get_template(key)

    def get_template(self, name):
        template = self.env.get_template(name)
        return Placeholder(template)

    def _ipython_key_completions_(self):
        return self.env.list_templates()
