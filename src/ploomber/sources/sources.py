import abc
import warnings
import re

from ploomber.products import Product
from ploomber.placeholders.Placeholder import Placeholder
from ploomber.exceptions import SourceInitializationError
from ploomber.sql import infer
from ploomber import static_analysis
from ploomber.static_analysis.string import StringExtractor


class Source(abc.ABC):
    """Source abstract class

    Sources encapsulate the code that will be executed. Tasks only focus on
    execution and sources take care of the source code lifecycle: import,
    render (add parameter and validate) then hand off execution to the Task
    object.

    They validation logic is optional and is done at init time to prevent
    Tasks from being instantiated with ill-defined sources. For example, a
    SQLScript is expected to create a table/view, if the source does not
    contain a CREATE TABLE/VIEW statement, then validation could prevent this
    source code from executing. Validation can also happen after rendering.
    For example, a PythonCallableSource checks that the passed Task.params are
    compatible with the source function signature.

    A new Task does not mean a new source is required, the concrete classes
    implemented cover most use cases, if none of the current implementations
    matches their use case, they can either use a GenericSource or implement
    their own.
    """
    @abc.abstractmethod
    def __init__(self, primitive, hot_reload=False):
        pass

    @property
    @abc.abstractmethod
    def primitive(self):
        """
        Return the argument passed to build the source, unmodified. e.g. For
        SQL code this is a string, for notebooks it is a JSON string (or
        a plain text code string in a formate that can be converted to a
        notebook), for PythonCallableSource, it is a callable object.

        Should load from disk each time the user calls source.primitive
        if hot_reload is True. Any other object build from the primitive
        (e.g. the code with the injected parameters) should access this
        so hot_reload is propagated.
        """
        # FIXME: there are some API inconsistencies. Most soruces just
        # return the raw argument that initialized them but NotebookSource
        # loads the file if it's a path
        pass

    # TODO: rename to params
    # NOTE: maybe allow dictionaries for cases where default values are
    # possible? python callables and notebooks
    @property
    @abc.abstractmethod
    def variables(self):
        pass

    @abc.abstractmethod
    def render(self, params):
        """Render source (fill placeholders)

        If hot_reload is True, this should indirectly reload primitive
        from disk by using self.primitive
        """
        pass

    # NOTE: maybe rename to path? the only case where it is not exactly a path
    # is when it is a callable (line number is added :line), but this is
    # intended as a human-readable property
    @property
    @abc.abstractmethod
    def loc(self):
        """
        Source location. For most cases, this is the path to the file that
        contains the source code. Used only for informative purpose (e.g.
        when doing dag.status())
        """
        pass

    @abc.abstractmethod
    def __str__(self):
        """
        Must return the code that will be executed by the Task in a
        human-readable form (even if it's not the actual code sent to the
        task, the only case where this happens currently is for notebooks,
        human-readable would be plain text code but the actual code is an
        ipynb file in JSON), if it is modified by the render step
        (e.g. SQL code with tags), calling this before rendering should
        raise an error.

        This and the task params given an unambiguous definition of which code
        will be run. And it's actually the same code that will be executed
        for all cases except notebooks (where the JSON string is passed to
        the task to execute).
        """
        pass

    # NOTE: should __repr__ contain: human-readable code + class name +
    # parameters? That's one option, we currently do not have a useful
    # __repr__ for sources

    @property
    @abc.abstractmethod
    def doc(self):
        """
        Returns code docstring
        """
        pass

    # TODO: rename to suffix to be consistent with pathlib.Path
    @property
    @abc.abstractmethod
    def extension(self):
        """
        Optional property that should return the file extension, used for
        code normalization (applied before determining wheter two pieces
        or code are different). If None, no normalization is done.
        """
        pass

    @abc.abstractmethod
    def _post_render_validation(self, rendered_value, params):
        """
        Validation function executed after rendering
        """
        pass

    @abc.abstractmethod
    def _post_init_validation(self, value):
        pass

    @property
    @abc.abstractmethod
    def name(self):
        pass

    # optional

    def extract_product(self):
        raise NotImplementedError('extract_product is not implemented in '
                                  '{}'.format(type(self).__name__))

    def extract_upstream(self):
        raise NotImplementedError('extract_upstream is not implemented in '
                                  '{}'.format(type(self).__name__))


class PlaceholderSource(Source):
    """
    Source concrete class for the ones that use a Placeholder
    """
    def __init__(self, value, hot_reload=False):
        self._primitive = value
        # rename, and make it private
        self._placeholder = Placeholder(value, hot_reload=hot_reload)
        self._post_init_validation(self._placeholder)

    @property
    def primitive(self):
        return self._primitive

    # TODO: rename to params
    @property
    def variables(self):
        return self._placeholder.variables

    def render(self, params):
        if params.get('upstream'):
            with params.get('upstream'):
                self._placeholder.render(params)
        else:
            self._placeholder.render(params)

        self._post_render_validation(str(self._placeholder), params)
        return self

    @property
    def loc(self):
        return self._placeholder.path

    def __str__(self):
        return str(self._placeholder)

    def __repr__(self):
        return "{}({})".format(
            type(self).__name__, self._placeholder.best_str(shorten=True))

    @property
    def name(self):
        if self._placeholder.path is not None:
            return self._placeholder.path.name


class SQLSourceMixin:
    @property
    def doc(self):
        regex = r'^\s*\/\*([\w\W]+)\*\/[\w\W]*'
        match = re.match(regex, self._placeholder.best_str(shorten=False))
        return '' if match is None else match.group(1)

    @property
    def extension(self):
        return 'sql'

    def extract_product(self):
        return static_analysis.sql.SQLExtractor(
            self._placeholder._raw).extract_product()

    def extract_upstream(self):
        return static_analysis.sql.SQLExtractor(
            self._placeholder._raw).extract_upstream()


class SQLScriptSource(SQLSourceMixin, PlaceholderSource):
    """
    A SQL (templated) script, it is expected to make a persistent change in
    the database (by using the CREATE statement), its validation verifies
    that, if no persistent changes should be validated use SQLQuerySource
    instead

    An object that represents SQL source, if a pathlib.Path object is passed,
    its contents are read and interpreted as the placeholder's content

    Notes
    -----
    This is really just a Placeholder object that stores its rendered
    version in the same object and raises an Exception if attempted. It also
    passes some of its attributes
    """
    def _post_init_validation(self, value):
        if not value.needs_render:
            raise SourceInitializationError(
                '{} cannot be initialized with literals as '
                'they are meant to create a persistent '
                'change in the database, they need to '
                'include the {} placeholder'.format(self.__class__.__name__,
                                                    '{{product}}'))

        # FIXME: validate {{product}} exists, does this also catch
        # {{product['key']}} ?

    def _post_render_validation(self, rendered_value, params):
        """Analyze code and warn if issues are found
        """
        if 'product' in params:
            infered_relations = infer.created_relations(rendered_value)
            # print(infered_relations)

            if isinstance(params['product'], Product):
                actual_rel = {params['product']._identifier}
            else:
                # metaproduct
                actual_rel = {p._identifier for p in params['product']}

            infered_len = len(infered_relations)
            # print(infered_len)
            actual_len = len(actual_rel)

            # print(set(infered_relations) != set(actual_rel),
            #         set(infered_relations) ,set(actual_rel))

            if not infered_len:
                warnings.warn(
                    'It seems like your task "{task}" will not create '
                    'any tables or views but the task has product '
                    '"{product}"'.format(task='some task',
                                         product=params['product']))

            elif infered_len != actual_len:
                warnings.warn('It seems like your task "{task}" will create '
                              '{infered_len} relation(s) but you declared '
                              '{actual_len} product(s): "{product}"'.format(
                                  task='some task',
                                  infered_len=infered_len,
                                  actual_len=actual_len,
                                  product=params['product']))
            # parsing infered_relations is still WIP
            # elif set(infered_relations) != set(infered_relations):
            #         warnings.warn('Infered relations ({}) did not match products'
            #                       ' {}'
            #                       .format(infered_relations, actual_len))

    def render(self, params):
        # FIXME: inefficient, initialize once and only update if needed
        # (i.e. hot reload is on)
        # NOTE: using private method because we don't want to raise an
        # exception if the value is None
        extracted = static_analysis.sql.SQLExtractor(
            self._placeholder._raw)._extract_product()
        # the code itself might already define the product, no need to pass it
        # TODO: verify that the product passed and the one defined are the same
        if extracted is not None:
            del params['product']
            params._dict[type(extracted).__name__] = type(extracted)

        return super().render(params)


class SQLQuerySource(SQLSourceMixin, PlaceholderSource):
    """
    Templated SQL query, it is not expected to make any persistent changes in
    the database (in contrast with SQLScriptSource), so its validation is
    different
    """

    # TODO: validate this is a SELECT statement
    # a query needs to return a result, also validate that {{product}}
    # does not exist in the template, instead of just making it optional

    def render(self, params):
        if params.get('upstream'):
            with params.get('upstream'):
                self._placeholder.render(params, optional=['product'])
        else:
            self._placeholder.render(params, optional=['product'])

        self._post_render_validation(str(self._placeholder), params)

    def _post_render_validation(self, rendered_value, params):
        pass

    def _post_init_validation(self, value):
        pass


class GenericSource(PlaceholderSource):
    """
    Generic source, the simplest type of source, it does not perform any kind
    of parsing nor validation

    Prameters
    ---------
    value: str
        The value for this source


    Notes
    -----
    value is directly passed to ploomber.templates.Placeholder, which means
    pathlib.Path objects are read and str are converted to jinja2.Template
    objects, for more details see Placeholder documentation
    """
    def __init__(self, value, hot_reload=False, optional=None, required=None):
        self._primitive = value
        # rename, and make it private
        self._placeholder = Placeholder(value,
                                        hot_reload=hot_reload,
                                        required=required)
        self._post_init_validation(self._placeholder)
        self._optional = optional
        self._required = required

    def render(self, params):
        if params.get('upstream'):
            with params.get('upstream'):
                self._placeholder.render(params, optional=self._optional)
        else:
            self._placeholder.render(params, optional=self._optional)

        self._post_render_validation(str(self._placeholder), params)
        return self

    @property
    def doc(self):
        return None

    @property
    def extension(self):
        return None

    def _post_render_validation(self, rendered_value, params):
        pass

    def _post_init_validation(self, value):
        pass


class FileSource(GenericSource):
    """
    A source that represents a path to a file, similar to GenericSource,
    but it casts the value argument to str, hence pathlib.Path will be
    interpreted as literals.

    This source is utilized by Tasks that move/upload files.

    """
    def __init__(self, value, hot_reload=False):
        # hot_reload does not apply here, ignored
        value = str(value)
        super().__init__(Placeholder(value))

    def render(self, params):
        if params.get('upstream'):
            with params.get('upstream'):
                self._placeholder.render(params,
                                         optional=['product', 'upstream'])
        else:
            self._placeholder.render(params, optional=['product', 'upstream'])

        self._post_render_validation(str(self._placeholder), params)

    def _post_render_validation(self, rendered_value, params):
        pass

    def _post_init_validation(self, value):
        pass

    def extract_upstream(self):
        return StringExtractor(self._placeholder._raw).extract_upstream()


class EmptySource(Source):
    """A source that does not do anything, used for sourceless tasks
    """
    def __init__(self, primitive, **kwargs):
        pass

    @property
    def primitive(self):
        pass

    @property
    def variables(self):
        pass

    def render(self, params):
        pass

    @property
    def loc(self):
        pass

    def __str__(self):
        return 'EmptySource'

    @property
    def doc(self):
        pass

    @property
    def extension(self):
        pass

    def _post_render_validation(self, rendered_value, params):
        pass

    def _post_init_validation(self, value):
        pass

    @property
    def name(self):
        pass
