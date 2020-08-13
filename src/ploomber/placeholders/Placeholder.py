from collections.abc import Mapping
import abc
import logging
from pathlib import Path

from ploomber.exceptions import RenderError, SourceInitializationError
from ploomber.placeholders import util
from jinja2 import (Environment, Template, UndefinedError, FileSystemLoader,
                    PackageLoader, StrictUndefined)


class AbstractPlaceholder(abc.ABC):
    """
    Placeholder are objects that can eventually be converted to strings using
    str(placeholder), if this operator is used before they are fully resolved
    (e.g. they need to render first), they will raise an error.

    repr(placeholder) is always safe to use and it will return the rendered
    version if available, otherwise, it will show the raw string with
    tags, the representation is shortened if needed.

    Placeholders are mostly used inside Source objects, but are sometims
    also used to give Products placeholding features (e.g.
    by directly using Placeholder or SQLRelationPlaceholder)
    """
    @abc.abstractmethod
    def __str__(self):
        pass

    @abc.abstractmethod
    def __repr__(self):
        pass

    @abc.abstractmethod
    def render(self, params):
        pass


class Placeholder(AbstractPlaceholder):
    """
    Placeholder powers all the objects that use placeholder variables (
    between curly brackets). It uses a jinja2.Template object under the hood
    but adds a few important things:

    * Keeps the raw (undendered) value: template.raw
    * Keeps path to raw value: template.location
    * Strict: will not render if missing or extra parameters
    * Upon calling .render, saves the rendered value for later access

    End users should not manipulate Placeholder objects, they should be
    automatically created from strings, pathlib.Path or jinja2.Template
    objects.

    Placeholder is mostly used by sources whose source code are parametrized
    strings (e.g. SQL scripts)

    Parameters
    ----------
    hot_reload : bool, optional
        Makes the placeholder always read the template from the file before
        rendering

    Attributes
    ----------
    variables : set
        Returns the set of variables in the template (values sourrounded by
        {{ and }})

    path : pathlib.Path
        The location of the raw object. None if initialized with a str or with
        a jinja2.Template created from a str

    """
    def __init__(self, primitive, hot_reload=False, required=None):
        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))
        self._hot_reload = hot_reload

        self._variables = None
        self.__template = None

        # we have to take care of 4 possible cases and make sure we have
        # all we need to initialize the template, this includes having
        # access to the raw template (str) and a way to re-initialize
        # the jinja.environment.loader object (to make sure copies and
        # pickles work)

        if isinstance(primitive, Path):
            self._path = primitive
            self.__raw = primitive.read_text()
            self._loader_init = None
        elif isinstance(primitive, str):
            self._path = None
            self.__raw = primitive
            self._loader_init = None

        elif isinstance(primitive, Template):
            path = Path(primitive.filename)

            if primitive.environment.undefined != StrictUndefined:
                raise ValueError('Placeholder can only be initialized '
                                 'from jinja2.Templates whose undefined '
                                 'parameter is set to '
                                 'jinja2.StrictUndefined, set it explicitely '
                                 'either in the Template or Environment '
                                 'constructors')

            # we cannot get the raw template on this case, raise error
            if not path.exists():
                raise ValueError('Could not load raw source from '
                                 'jinja2.Template, this usually happens '
                                 'when Templates are initialized directly '
                                 'from a str, only Templates loaded from '
                                 'the filesystem are supported, using a '
                                 'FileSystemLoader or '
                                 'PackageLoader will fix this issue, '
                                 'if you want to create a template from '
                                 'a string pass it directly '
                                 'Placeholder("some {{placeholder}}")')

            self._path = path
            self.__raw = path.read_text()
            self._loader_init = _make_loader_init(primitive.environment.loader)
        # SourceLoader returns Placeholder objects, which could inadvertedly
        # be passed to another Placeholder constructor when instantiating
        # a source object, since they sometimes use placeholders
        #  make sure this case is covered
        elif isinstance(primitive, Placeholder):
            self._path = primitive.path
            self.__raw = primitive._raw
            self._loader_init = _make_loader_init(
                primitive._template.environment.loader)
        else:
            raise TypeError('{} must be initialized with a Template, '
                            'Placeholder, pathlib.Path or str, '
                            'got {} instead'.format(
                                type(self).__name__,
                                type(primitive).__name__))

        if self._path is None and hot_reload:
            raise ValueError('hot_reload only works when Placeholder is '
                             'initialized from a file')

        # TODO: remove
        self.needs_render = self._needs_render()

        self._str = None if self.needs_render else self._raw

        if required:
            self._validate_required(required)

    def _validate_required(self, required):
        missing_required = set(required) - self.variables

        if missing_required:
            msg = ('The following tags are required. ' +
                   display_error(missing_required, required))
            raise SourceInitializationError(msg)

    @property
    def _template(self):
        if self.__template is None or self._hot_reload:
            self.__template = _init_template(self._raw, self._loader_init)

        return self.__template

    @property
    def _raw(self):
        """A string with the raw jinja2.Template contents
        """
        if self._hot_reload:
            self.__raw = self._path.read_text()

        return self.__raw

    @_raw.setter
    def _raw(self, value):
        self.__raw = value

    @property
    def path(self):
        return self._path

    def _needs_render(self):
        """
        Returns true if the template is a literal and does not need any
        parameters to render
        """
        env = self._template.environment

        # check if the template has the variable or block start string
        # is there any better way of checking this?
        needs_variables = (env.variable_start_string in self._raw
                           and env.variable_end_string in self._raw)
        needs_blocks = (env.block_start_string in self._raw
                        and env.block_end_string in self._raw)

        return needs_variables or needs_blocks

    def __str__(self):
        if self._str is None:
            raise RuntimeError('Tried to read {} {} without '
                               'rendering first'.format(
                                   type(self).__name__, repr(self)))

        return self._str

    def render(self, params, optional=None, required=None):
        """
        """
        optional = optional or set()
        optional = set(optional)

        passed = set(params.keys())

        missing = self.variables - passed
        extra = passed - self.variables - optional

        # FIXME: self.variables should also be updated on hot_reload
        if missing:
            raise RenderError('in {}, missing required '
                              'parameters: {}, params passed: {}'.format(
                                  repr(self), missing, params))

        if extra:
            raise RenderError('in {}, unused parameters: {}, params '
                              'declared: {}'.format(repr(self), extra,
                                                    self.variables))

        try:
            self._str = self._template.render(**params)
        except UndefinedError as e:
            # TODO: we can use e.message to see which param caused the
            # error
            raise RenderError('in {}, jinja2 raised an UndefinedError, this '
                              'means the template is using an attribute '
                              'or item that does not exist, the original '
                              'traceback is shown above. For jinja2 '
                              'implementation details see: '
                              'http://jinja.pocoo.org/docs/latest'
                              '/templates/#variables'.format(
                                  repr(self))) from e

        return str(self)

    def best_str(self, shorten):
        """
        Returns the rendered version (if available), otherwise the raw version
        """
        best = self._raw if self._str is None else self._str

        if shorten:
            lines = best.split('\n')
            first_line = lines[0]
            best = first_line if len(first_line) < 80 else first_line[:77]

            if len(lines) > 1 or len(first_line) >= 80:
                best += '...'

        return best

    @property
    def variables(self):
        """Returns declared variables in the template
        """
        # this requires parsing the raw template, do lazy load, but override
        # it if hot_reload is True
        if self._variables is None or self._hot_reload:
            self._variables = util.get_tags_in_str(self._raw)

        return self._variables

    def __repr__(self):
        return '{}("{}")'.format(
            type(self).__name__, self.best_str(shorten=True))

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_logger']
        del state['_Placeholder__template']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))

        self.__template = None


def _init_template(raw, loader_init):
    """
    Initializes template, taking care of configuring the loader environment
    if needed

    This helps prevents errors when using copy or pickling (the copied or
    unpickled object wont have access to the environment.loader which breaks
    macros and anything that needs access to the jinja environment.loader
    object
    """
    if loader_init is None:
        return Template(raw, undefined=StrictUndefined)
    else:
        if loader_init['class'] == 'FileSystemLoader':
            loader = FileSystemLoader(**loader_init['kwargs'])
        elif loader_init['class'] == 'PackageLoader':
            loader = PackageLoader(**loader_init['kwargs'])
        else:
            raise TypeError('Error setting state for Placeholder, '
                            'expected the loader to be FileSystemLoader '
                            'or PackageLoader')

        env = Environment(loader=loader, undefined=StrictUndefined)
        return env.from_string(raw)


def _get_package_name(loader):
    # the provider attribute was introduced in jinja 2.11.2, previous
    # versions have package_name
    if hasattr(loader, 'package_name'):
        return loader.package_name
    else:
        return loader.provider.loader.name


def _make_loader_init(loader):

    if loader is None:
        return None
    if isinstance(loader, FileSystemLoader):
        return {
            'class': type(loader).__name__,
            'kwargs': {
                'searchpath': loader.searchpath
            }
        }
    elif isinstance(loader, PackageLoader):
        return {
            'class': type(loader).__name__,
            'kwargs': {
                'package_name': _get_package_name(loader),
                'package_path': loader.package_path
            }
        }
    else:
        raise TypeError('Only templates with loader type '
                        'FileSystemLoader or PackageLoader are '
                        'supported, got: {}'.format(type(loader).__name__))


class SQLRelationPlaceholder(AbstractPlaceholder):
    """
    Sources are also used by some Products that support placeholders, such as
    SQLRelation or File.

    An structured Placeholder to represents a database relation (table or
    view). Used by Products that take SQL relations as parameters.
    Not meant to be used directly by end users.

    Parameters
    ----------
    source: tuple
      A (schema, name, kind) or a (name, kind) tuple, where kind is either
      'table' or 'view'


    Notes
    -----
    Beware that SQLRelationPlaceholder(('schema', 'data', 'table')) is
    different from SQLRelationPlaceholder(('"schema"', '"data"', '"table"')).
    The first one will be rendered as schema.data and the second one as
    "schema"."data" (in SQL, quoted identifiers are case-sensitive)
    """

    # TODO: allow templating in schema

    def __init__(self, source):
        if len(source) == 2:
            name, kind = source
            schema = None
        elif len(source) == 3:
            schema, name, kind = source
        else:
            raise ValueError('{} must be initialized with 2 or 3 elements, '
                             'got: {}'.format(
                                 type(self).__name__, len(source)))

        # ignore empty string
        if schema == '':
            schema = None

        if name is None:
            raise ValueError('name cannot be None')

        if kind not in ('view', 'table'):
            raise ValueError('kind must be one of ["view", "table"] '
                             'got "{}"'.format(kind))

        self._schema = schema
        self._name_template = Placeholder(name)
        self._kind = kind

        # if source is literal, rendering without params should work, this
        # allows this template to be used without having to render the dag
        # first
        if not self._name_template.needs_render:
            self._name_template.render({})

    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        return str(self._name_template)

    @property
    def kind(self):
        return self._kind

    # FIXME: THIS SHOULD ONLY BE HERE IF POSTGRES

    def _validate_name(self, name):
        if len(name) > 63:
            url = ('https://www.postgresql.org/docs/current/'
                   'sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS')
            raise ValueError('"{}" exceeds maximum length of 63 '
                             ' (length is {}), '
                             'see: {}'.format(name, len(name), url))

    def render(self, params, **kwargs):
        name = self._name_template.render(params, **kwargs)
        self._validate_name(name)
        return str(self)

    def _qualified_name(self, unrendered_ok, shorten):
        qualified = ''

        if self.schema is not None:
            qualified += self.schema + '.'

        if unrendered_ok:
            qualified += self._name_template.best_str(shorten=shorten)
        else:
            qualified += str(self._name_template)

        return qualified

    def __str__(self):
        return self._qualified_name(unrendered_ok=False, shorten=False)

    def __repr__(self):
        return ('SQLRelationPlaceholder({})'.format(
            self._qualified_name(unrendered_ok=True, shorten=True)))

    def best_str(self, shorten):
        return self._qualified_name(unrendered_ok=True, shorten=shorten)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))


def display_error(keys, descriptions):
    if isinstance(descriptions, Mapping):
        msg = '\n'

        for key, error in descriptions.items():
            msg += '"{key}": {error}\n'.format(key=key, error=error)

        return msg

    else:
        return ', '.join(keys)
