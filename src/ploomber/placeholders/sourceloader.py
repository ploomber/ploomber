import pydoc
from pathlib import Path, PurePosixPath
from ploomber.placeholders.placeholder import Placeholder
from ploomber.placeholders import extensions
from ploomber.util.util import isiterable_not_str

from jinja2 import Environment, FileSystemLoader, StrictUndefined, exceptions


def _is_iterable_w_types(o, types):
    for element in o:
        if not isinstance(o, types):
            return False
    return True


class SourceLoader:
    """Load source files using a jinja2.Environment

    Data pipelines usually rely on non-Python source code such as SQL scripts,
    SourceLoader provides a convenient way of loading them. This serves two
    purposes: 1) Avoid hardcoded paths to files and 2) Allows using advanced
    jinja2 features that require an Environment object such as macros (under
    the hood, SourceLoader initializes an Environment with a FileSystemLoader)
    SourceLoader returns ploomber.Placeholder objects that can be directly
    passed to Tasks in the source parameter

    Parameters
    ----------
    path : str, pathlib.Path, optional
        Path (or list of) to load files from. Required if module is None

    module : str or module, optional
        Module name as dotted string or module object. Preprends to path
        parameter

    Examples
    --------
    >>> from ploomber import SourceLoader
    >>> loader = SourceLoader('path/to/templates/')
    >>> loader['load_customers.sql'] # doctest: +SKIP
    >>> loader.get_template('load_customers.sql') # doctest: +SKIP
    """

    def __init__(self, path=None, module=None):
        if path is None and module is None:
            raise TypeError("Path cannot be None if module is None")

        # validate path
        if isiterable_not_str(path):
            if _is_iterable_w_types(path, (str, Path)):
                types_found = set(type(element) for element in path)
                raise TypeError(
                    "If passing an iterable, path must consist "
                    "of str and pathlib.Path objects only, got "
                    "{}".format(types_found)
                )

            path = [str(element) for element in path]
        elif isinstance(path, Path):
            path = str(path)

        if isinstance(module, str):
            module_obj = pydoc.locate(module)

            if module_obj is None:
                raise ValueError('Could not locate module "{}"'.format(module))

            module = module_obj

        # for module objects
        if hasattr(module, "__file__"):
            module_path = str(Path(module.__file__).parent.resolve())

        elif module is None:
            module_path = ""
        else:
            raise ValueError(
                "Could not find module path, pass a string or a " "module object"
            )

        if isiterable_not_str(path):
            self.path_full = [str(Path(module_path, e)) for e in path]
        else:
            # if path is None, do not append anything
            self.path_full = str(Path(module_path, path or ""))

        self.env = self.__init_env()

    def __init_env(self):
        # NOTE: we do not use jinja2.PackageLoader since it does not provide
        # the abilty to pass a list of paths
        return Environment(
            loader=FileSystemLoader(self.path_full),
            # this will cause jinja2 to raise an exception if a variable
            # declared in the template is not passed in the render parameters
            undefined=StrictUndefined,
            extensions=(extensions.RaiseExtension,),
        )

    def __getitem__(self, key):
        return self.get_template(key)

    def get(self, key):
        """Load template, returns None if it doesn' exist"""
        try:
            return self[key]
        except exceptions.TemplateNotFound:
            return None

    def path_to(self, key):
        """Return the path to a template, even if it doesn't exist"""
        try:
            return self[key].path
        except exceptions.TemplateNotFound:
            return Path(self.path_full, key)

    def get_template(self, name):
        """Load a template by name

        Parameters
        ----------
        name : str or pathlib.Path
            Template to load
        """
        # if name is a nested path, this will return an appropriate
        # a/b/c string even on Windows
        path = str(PurePosixPath(*Path(name).parts))

        try:
            template = self.env.get_template(path)
        except exceptions.TemplateNotFound as e:
            exception = e
        else:
            exception = None

        if exception is not None:
            expected_path = str(Path(self.path_full, name))

            # user saved the template locally, but the source loader is
            # configured to load from a different place
            if Path(name).exists():
                raise exceptions.TemplateNotFound(
                    f"{str(name)!r} template does not exist. "
                    "However such a file exists in the current working "
                    "directory, if you want to load it as a template, move it "
                    f"to {self.path_full!r} or remove the source_loader"
                )
            # no template and the file does not exist, raise a generic message
            else:
                raise exceptions.TemplateNotFound(
                    f"{str(name)!r} template does not exist. "
                    "Based on your configuration, if should be located "
                    f"at: {expected_path!r}"
                )

        return Placeholder(template)

    def _ipython_key_completions_(self):
        return self.env.list_templates()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["env"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.env = self.__init_env()
