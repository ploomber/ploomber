import logging
from pathlib import Path

from ploomber.exceptions import RenderError
from ploomber.templates import util

from numpydoc.docscrape import NumpyDocString
import jinja2
from jinja2 import (Environment, Template, UndefinedError,
                    FileSystemLoader, PackageLoader)


class Placeholder:
    """
    A jinja2 Template-like object that adds the following features:

        * template.raw - Returns the raw template used for initialization
        * template.location - Returns a path to the Template object if available
        * strict - will not render if missing or extra parameters
        * It also keeps the rendered value for later access

    Note that this does not implement the full jinja2.Template API
    """

    def __init__(self, source):
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

        if isinstance(source, Path):
            self._path = source
            self._raw = source.read_text()
            self._template = Template(self._raw,
                                      undefined=jinja2.StrictUndefined)
        elif isinstance(source, str):
            self._path = None
            self._raw = source
            self._template = Template(self._raw,
                                      undefined=jinja2.StrictUndefined)

        elif isinstance(source, Template):
            path = Path(source.filename)

            if source.environment.undefined != jinja2.StrictUndefined:
                raise ValueError('Placeholder can only be initialized '
                                 'from jinja2.Templates whose undefined '
                                 'parameter is set to '
                                 'jinja2.StrictUndefined, set it explicitely '
                                 'either in the Template or Environment '
                                 'constructors')

            if not path.exists():
                raise ValueError('Could not load raw source from '
                                 'jinja2.Template, this usually happens '
                                 'when Templates are initialized directly '
                                 'from a str, only Templates loaded from '
                                 'the filesystem are supported, using a '
                                 'jinja2.Environment will fix this issue, '
                                 'if you want to create a template from '
                                 'a string pass it directly to this '
                                 'constructor')

            self._path = path
            self._raw = path.read_text()
            self._template = source
        elif isinstance(source, Placeholder):
            self._path = source.path
            self._raw = source.raw
            self._template = source.template
        else:
            raise TypeError('{} must be initialized with a Template, '
                            'Placeholder, pathlib.Path or str, '
                            'got {} instead'
                            .format(type(self).__name__,
                                    type(source).__name__))

        self.declared = self._get_declared()

        self.needs_render = self._needs_render()

        self._value = None if self.needs_render else self.raw

        loader = self._template.environment.loader

        if loader is not None:
            if isinstance(loader, FileSystemLoader):
                self.loader_init = {'class': type(loader).__name__,
                                    'kwargs':
                                    {'searchpath': loader.searchpath}}
            elif isinstance(loader, PackageLoader):
                self.loader_init = {'class': type(loader).__name__,
                                    'kwargs': {
                                    'package_name': loader.provider.loader.name,
                                    'package_path': loader.package_path}}
            else:
                raise TypeError('Only templates with loader tyoe '
                                'FileSystemLoader or PackageLoader are '
                                'supported, got: {}'
                                .format(type(loader).__name__))
        else:
            self.loader_init = None

        # dynamically set the docstring
        # self.__doc__ = self._parse_docstring()

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
        return self._raw if self._value is None else self._value

    @property
    def template(self):
        """jinja2.Template object
        """
        return self._template

    @property
    def raw(self):
        """A string with the raw jinja2.Template contents
        """
        return self._raw

    @property
    def path(self):
        """The location of the raw object

        Notes
        -----
        None if initialized with a str or with a jinja2.Template created
        from a str
        """
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

    def _get_declared(self):
        return util.get_tags_in_str(self.raw)

    def diagnose(self):
        """Prints some diagnostics
        """
        found = self.declared
        docstring_np = NumpyDocString(self.docstring())
        documented = set([p[0] for p in docstring_np['Parameters']])

        print('The following variables were found in the template but are '
              'not documented: {}'.format(found - documented))

        print('The following variables are documented but were not found in '
              'the template: {}'.format(documented - found))

        return documented, found

    def render(self, params, optional=None):
        """
        """
        optional = optional or {}
        optional = set(optional)

        passed = set(params.keys())

        missing = self.declared - passed
        extra = passed - self.declared - optional

        if missing:
            raise RenderError('in {}, missing required '
                              'parameters: {}, params passed: {}'
                              .format(repr(self), missing, params))

        if extra:
            raise RenderError('in {}, unused parameters: {}, params '
                              'declared: {}'
                              .format(repr(self), extra, self.declared))

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

        if self.loader_init is None:
            self._template = Template(self.raw,
                                      undefined=jinja2.StrictUndefined)
        # re-construct the Templates environment, otherwise there could
        # be errors when using copy or pickling (the copied or unpickled
        # object wont have access to the environment which can break macros
        # and other thigns)
        else:
            if self.loader_init['class'] == 'FileSystemLoader':
                loader = FileSystemLoader(**self.loader_init['kwargs'])
            elif self.loader_init['class'] == 'PackageLoader':
                loader = PackageLoader(**self.loader_init['kwargs'])
            else:
                raise TypeError('Error setting state for Placeholder, '
                                'expected the loader to be FileSystemLoader '
                                'or PackageLoader')

            env = Environment(loader=loader, undefined=jinja2.StrictUndefined)
            self._template = env.from_string(self.raw)

    @property
    def name(self):
        if self._path is None:
            raise AttributeError('Cannot get name for Placeholder if '
                                 'initialized directly from a string, load '
                                 'from a pathlib.Path or using '
                                 'ploomber.SourceLoader for this to work')
        else:
            return self._path.name


class SQLRelationPlaceholder:
    """
    An identifier that represents a database relation (table or view), used
    internally by SQLiteRelation (Product). Not meant to be used directly
    by users.

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
