"""
If Task B has Task A as a dependency, it means that the
Product of A should be used by B in some way (e.g. Task A produces a table
and Task B pivots it), placeholders help avoid redundancy when building tasks,
if you declare that Product A is "schema"."table", the use of placeholders
prevents "schema"."table" to be explicitely declared in B, since B depends
on A, this information from A is passed to B. Sources fill that purpose,
they are placeholders that will be filled at rendering time so that
parameteters are only declared once.

They serve a second, more advanced use case. It is recommended for Tasks to
have no parameters and be fully declared by specifying their code, product
and upstream dependencies. However, there is one use case where parameters
are useful: batch processing and parallelization. For example, if we are
operating on a 10-year databse, a single task might take too long, but we
could split the data in 1-year chunks and process them in parallel, in such
use case we could create 10 task instances, each one with a different year
parameters and process them independently. So, apart from upstream and product
placeholders, arbitrary parameters can also be placeholders.

These classes are not intended to be used by the end user, since Task and
Product objects create sources from strings.
"""
import abc
import warnings
import re
import inspect

from ploomber.products import Product
from ploomber.templates.Placeholder import Placeholder
from ploomber.exceptions import SourceInitializationError
from ploomber.sql import infer

# FIXME: move diagnose to here, task might need this as well, since
# validation may involve checking against the product, but we can replace
# this behabior for an after-render validation, and just pass the product
# as parameter maybe from the Task? the task should not do this
# FIXME: remove opt from Placeholder.render


class Source(abc.ABC):
    """Source abstract class

    Sources encapsulate the code that will be executed by Tasks, they add the
    ability to render placeholders so Products are only declared once.

    They optionally implement validation logic that Tasks can use in two
    scenarios: initialization and render. Initialization prevents Tasks
    from being instantiated with ill-defined sources. For example, a SQLScript
    is expected to create a table/view, if the source does not contains a
    CREATE TABLE/VIEW statement, then validation could fail to prevent this
    error be to be discovered at runtime. Render validation happens after
    placeholders are resolved. For example, a PythonCallable checks that
    the passed Task.params are compatible with the source function signature.

    Since there is a limited amount of Sources that cover most use cases, this
    will help developers to create their own Tasks faster, as they can pick up
    one of the implemented sources, if none of the current implementations
    matches their use case, they can either use a GenericSource or as a last
    resource, implement their own.

    Sources are also used by some Products that support placeholders, such as
    SQLRelation or File.

    """

    def __init__(self, value):
        self.value = Placeholder(value)
        self._post_init_validation(self.value)

    @property
    def variables(self):
        return self.value.variables

    @property
    def needs_render(self):
        """
        Whether this source needs render (because it has {{}} placeholders
        or not). Some Tasks do not accept sources that have placeholders
        and they can use this function to raise errors during initialization
        """
        return self.value.needs_render

    def render(self, params):
        """Render source (fill placeholders)
        """
        self.value.render(params)
        self._post_render_validation(self.value.value, params)
        return self

    @property
    def loc(self):
        """
        Source location. For most cases, this is the path to the file that
        contains the source code. Used only for informative purpose (e.g.
        when doing dag.status())
        """
        return self.value.path

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "{}({})".format(type(self).__name__, self.value._value_repr)

    @property
    def doc_short(self):
        """Returns the first line in the docstring, None if no docstring
        """
        if self.doc is not None:
            return self.doc.split('\n')[0]
        else:
            return None

    @property
    def extension(self):
        """
        Optional property that should return the file extension, used for
        code normalization (applied before determining wheter two pieces
        or code are different). If None, no normalization is done.
        """
        return None

    # optional validation

    def _post_render_validation(self, rendered_value, params):
        """
        Validation function executed after rendering
        """
        pass

    def _post_init_validation(self, value):
        pass

    # required by subclasses

    @property
    @abc.abstractmethod
    def doc(self):
        """
        Returns code docstring
        """
        pass


class SQLSourceMixin:
    """A source representing SQL source code
    """

    @property
    def doc(self):
        regex = r'^\s*\/\*([\w\W]+)\*\/[\w\W]*'
        match = re.match(regex, self.value.safe)
        return '' if match is None else match.group(1)

    @property
    def extension(self):
        return 'sql'


class SQLScriptSource(SQLSourceMixin, Source):
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
                'include the {} placeholder'
                .format(self.__class__.__name__, '{{product}}'))

        # FIXME: validate {{product}} exists, does this also catch
        # {{product['key']}} ?

    def _post_render_validation(self, rendered_value, params):
        """Analyze code and warn if issues are found
        """
        # print(params)
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
            warnings.warn('It seems like your task "{task}" will not create '
                          'any tables or views but the task has product '
                          '"{product}"'
                          .format(task='some task',
                                  product=params['product']))

        elif infered_len != actual_len:
            warnings.warn('It seems like your task "{task}" will create '
                          '{infered_len} relation(s) but you declared '
                          '{actual_len} product(s): "{product}"'
                          .format(task='some task',
                                  infered_len=infered_len,
                                  actual_len=actual_len,
                                  product=params['product']))
        # parsing infered_relations is still WIP
        # elif set(infered_relations) != set(infered_relations):
        #         warnings.warn('Infered relations ({}) did not match products'
        #                       ' {}'
        #                       .format(infered_relations, actual_len))


class SQLQuerySource(SQLSourceMixin, Source):
    """
    Templated SQL query, it is not expected to make any persistent changes in
    the database (in contrast with SQLScriptSource), so its validation is
    different
    """
    # TODO: validate this is a SELECT statement
    # a query needs to return a result, also validate that {{product}}
    # does not exist in the template, instead of just making it optional

    def render(self, params):
        self.value.render(params, optional=['product'])
        self._post_render_validation(self.value.value, params)


class PythonCallableSource(Source):
    """A source that holds a Python callable
    """

    def __init__(self, value):
        if not callable(value):
            raise TypeError('{} must be initialized'
                            'with a Python callable, got '
                            '"{}"'
                            .format(type(self).__name__),
                            type(value).__name__)

        self.value = value
        self._source_as_str = inspect.getsource(value)
        _, self._source_lineno = inspect.getsourcelines(value)
        self._loc = inspect.getsourcefile(value)

    def __str__(self):
        return self._source_as_str

    @property
    def doc(self):
        return self.value.__doc__

    @property
    def loc(self):
        return '{}:{}'.format(self._loc, self._source_lineno)

    @property
    def needs_render(self):
        return False

    @property
    def extension(self):
        return 'py'


class GenericSource(Source):
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
    @property
    def doc(self):
        return None


class FileSource(GenericSource):
    """
    A source that represents a path to a file, similar to GenericSource,
    but it casts the value arument to str, hence pathlib.Path will be
    interpreted as literals.

    This source is utilized by Tasks that move/upload files.
    """

    def __init__(self, value):
        value = str(value)
        super().__init__(Placeholder(value))

    def render(self, params):
        self.value.render(params, optional=['product'])
        self._post_render_validation(self.value.value, params)
