import logging
from pathlib import Path

from ploomber.exceptions import RenderError
from ploomber.templates import util

import jinja2
from jinja2 import (Environment, Template, UndefinedError,
                    FileSystemLoader, PackageLoader)


class Placeholder:
    """
    Placeholder powers all the objects that use placeholder variables (
    between curly brackets). It uses a jinja2.Template object under the hood
    but adds a few important things:

    * Keeps the raw (undendered) value: template.raw
    * Keeps path to raw value: template.location
    * Strict: will not render if missing or extra parameters
    * Upon calling .render, saves the rendered value for later access

    Although this object mimics a jinja2.Template object, it does not implement
    the full API.

    End users should not manipulate Placeholder objects, they should be
    automatically created from strings, pathlib.Path or jinja2.Template
    objects.

    Parameters
    ----------
    hot_reload : bool, optional
        Makes the placeholder always read the template from the file before
        rendering

    Attributes
    ----------
    path
        The location of the raw object. None if initialized with a str or with
        a jinja2.Template created from a str
    """

    def __init__(self, source, hot_reload=False):
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))
        self._hot_reload = hot_reload

        # the public versions of this attributes are lazily loaded
        self._variables = None
        self._template = None

        # we have to take care of 4 possible cases and make sure we have
        # all we need to initialize the template, this includes having
        # access to the raw template (str) and a way to re-initialize
        # the jinja.environment.loader object (to make sure copies and
        # pickles work)

        if isinstance(source, Path):
            self._path = source
            self._raw = source.read_text()
            self._loader_init = None
        elif isinstance(source, str):
            self._path = None
            self._raw = source
            self._loader_init = None

        elif isinstance(source, Template):
            path = Path(source.filename)

            if source.environment.undefined != jinja2.StrictUndefined:
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
            self._raw = path.read_text()
            self._loader_init = _make_loader_init(source.environment.loader)
        # SourceLoader returns Placeholder objects, which could inadvertedly
        # be passed to another Placeholder constructor when instantiating
        # a source object, make sure this case is covered
        elif isinstance(source, Placeholder):
            self._path = source.path
            self._raw = source.raw
            self._loader_init = _make_loader_init(source
                                                  .template.environment.loader)
        else:
            raise TypeError('{} must be initialized with a Template, '
                            'Placeholder, pathlib.Path or str, '
                            'got {} instead'
                            .format(type(self).__name__,
                                    type(source).__name__))

        if self._path is None and hot_reload:
            raise ValueError('hot_reload only works when Placeholder is '
                             'initialized from a file')

        self.needs_render = self._needs_render()

        self._value = None if self.needs_render else self.raw

    @property
    def variables(self):
        """Returns declared variables in the template
        """
        # this requires parsing the raw template, do lazy load, but override
        # it if hot_reload is True
        if self._variables is None or self._hot_reload:
            self._variables = util.get_tags_in_str(self.raw)

        return self._variables

    @property
    def value(self):
        if self._value is None:
            raise RuntimeError('Tried to read {} {} without '
                               'rendering first'
                               .format(type(self).__name__,
                                       repr(self)))

        return self._value

    @property
    def _value_repr(self):
        raw = self._raw if self._value is None else self._value
        parts = raw.split('\n')
        first_part = parts[0]
        short = first_part if len(first_part) < 80 else first_part[:77]

        if len(parts) > 1 or len(first_part) >= 80:
            short += '...'
        return short

    @property
    def template(self):
        """jinja2.Template object
        """
        if self._template is None or self._hot_reload:
            self._template = _init_template(self.raw, self._loader_init)

        return self._template

    @property
    def raw(self):
        """A string with the raw jinja2.Template contents
        """
        if self._hot_reload:
            self._raw = self._path.read_text()

        return self._raw

    @raw.setter
    def raw(self, value):
        self._raw = value

    @property
    def path(self):
        return self._path

    def _needs_render(self):
        """
        Returns true if the template is a literal and does not need any
        parameters to render
        """
        env = self.template.environment

        # check if the template has the variable or block start string
        # is there any better way of checking this?
        needs_variables = (env.variable_start_string in self.raw
                           and env.variable_end_string in self.raw)
        needs_blocks = (env.block_start_string in self.raw
                        and env.block_end_string in self.raw)

        return needs_variables or needs_blocks

    def __str__(self):
        return self.value

    def __repr__(self):
        return '{}("{}")'.format(type(self).__name__, self.safe)

    def render(self, params, optional=None):
        """
        """
        optional = optional or {}
        optional = set(optional)

        passed = set(params.keys())

        missing = self.variables - passed
        extra = passed - self.variables - optional

        # FIXME: self.variables should also be updated on hot_reload
        if missing:
            raise RenderError('in {}, missing required '
                              'parameters: {}, params passed: {}'
                              .format(repr(self), missing, params))

        if extra:
            raise RenderError('in {}, unused parameters: {}, params '
                              'declared: {}'
                              .format(repr(self), extra, self.variables))

        try:
            self._value = self.template.render(**params)
            return self.value
        except UndefinedError as e:
            raise RenderError('in {}, jinja2 raised an UndefinedError, this '
                              'means the template is using an attribute '
                              'or item that does not exist, the original '
                              'traceback is shown above. For jinja2 '
                              'implementation details see: '
                              'http://jinja.pocoo.org/docs/latest'
                              '/templates/#variables'
                              .format(repr(self))) from e

    @property
    def safe(self):
        if self._value is None:
            return self.raw
        else:
            return self._value

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger and _source are not pickable, so we remove them and build
        # them again in __setstate__
        del state['_logger']
        del state['_template']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

        # make sure this attribute exists, we deleted it in getstate
        self._template = None

    @property
    def name(self):
        if self._path is None:
            raise AttributeError('Cannot get name for Placeholder if '
                                 'initialized directly from a string, load '
                                 'from a pathlib.Path or using '
                                 'ploomber.SourceLoader for this to work')
        else:
            return self._path.name


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
        return Template(raw, undefined=jinja2.StrictUndefined)
    else:
        if loader_init['class'] == 'FileSystemLoader':
            loader = FileSystemLoader(**loader_init['kwargs'])
        elif loader_init['class'] == 'PackageLoader':
            loader = PackageLoader(**loader_init['kwargs'])
        else:
            raise TypeError('Error setting state for Placeholder, '
                            'expected the loader to be FileSystemLoader '
                            'or PackageLoader')

        env = Environment(loader=loader, undefined=jinja2.StrictUndefined)
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
        return {'class': type(loader).__name__,
                'kwargs':
                {'searchpath': loader.searchpath}}
    elif isinstance(loader, PackageLoader):
        return {'class': type(loader).__name__,
                'kwargs': {
            'package_name': _get_package_name(loader),
            'package_path': loader.package_path}}
    else:
        raise TypeError('Only templates with loader type '
                        'FileSystemLoader or PackageLoader are '
                        'supported, got: {}'
                        .format(type(loader).__name__))


class SQLRelationPlaceholder:
    """
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
                             'got: {}'
                             .format(type(self).__name__, len(source)))

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
        return self._name_template.value

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
        return self

    def _get_qualified(self, allow_repr=False):
        qualified = ''

        if self.schema is not None:
            qualified += self.schema + '.'

        if allow_repr:
            qualified += self._name_template._value_repr
        else:
            qualified += self._name_template.value

        return qualified

    def __str__(self):
        return self._get_qualified(allow_repr=False)

    def __repr__(self):
        return ('SQLRelationPlaceholder({})'
                .format(self._get_qualified(allow_repr=True)))

    # TODO: rename this
    @property
    def safe(self):
        return self._get_qualified(allow_repr=True)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))
