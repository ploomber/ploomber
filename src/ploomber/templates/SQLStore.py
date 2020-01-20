"""
This module provides subclasses of jinja2 for better integration with SQL
"""
from ploomber.templates.Placeholder import Placeholder

import jinja2
from jinja2 import Environment, PackageLoader, FileSystemLoader


class SQLStore:
    """
    Utility class for loading SQL files from a folder, supports parametrized
    SQL templates (jinja2)

    Examples
    --------
    >>> from tax_estimator.sql import SQLStore
    >>> from ploomber import Env
    >>> env = Env()
    >>> path = env.path.home / 'load' / 'sql'
    >>> sqlstore = SQLStore(path)
    """

    def __init__(self, module, path):
        if module is None:
            loader = FileSystemLoader(path)
        else:
            loader = PackageLoader(module, path)

        self.env = Environment(
            loader=loader,
            # this will cause jinja2 to raise an exception if a variable
            # declared in the template is not passed in the render parameters
            undefined=jinja2.StrictUndefined)

    def __dir__(self):
        return [t for t in self.env.list_templates() if t.endswith('.sql')]

    def get_template(self, name):
        template = self.env.get_template(name)
        return Placeholder(template)
