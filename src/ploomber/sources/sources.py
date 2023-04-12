import warnings

from ploomber.products import Product
from ploomber.placeholders.placeholder import Placeholder
from ploomber.exceptions import SourceInitializationError
from ploomber import static_analysis
from ploomber.static_analysis.string_ import StringExtractor
from ploomber.sources import docstring, abc


class PlaceholderSource(abc.Source):
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
        if params.get("upstream"):
            with params.get("upstream"):
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
        repr_ = "{}({})".format(
            type(self).__name__, self._placeholder.best_repr(shorten=True)
        )

        if self._placeholder.path:
            repr_ += " Loaded from: {}".format(self._placeholder.path)

        return repr_

    @property
    def name(self):
        if self._placeholder.path is not None:
            # filename without extension(e.g., plot.py -> plot)
            return self._placeholder.path.stem


class SQLSourceMixin:
    @property
    def doc(self):
        return docstring.extract_from_sql(self._placeholder.best_repr(shorten=False))

    @property
    def extension(self):
        return "sql"

    def extract_product(self):
        return static_analysis.sql.SQLExtractor(self._placeholder).extract_product()

    def extract_upstream(self):
        return static_analysis.sql.SQLExtractor(self._placeholder).extract_upstream()


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

    def __init__(self, value, hot_reload=False, split_source=None):
        self._split_source = split_source
        super().__init__(value, hot_reload)

    def _post_init_validation(self, value):
        if not value.needs_render:
            example = "CREATE TABLE {{product}} AS (SELECT * FROM ...)"
            raise SourceInitializationError(
                f"Error initializing {self!r}: "
                "The {{product}} placeholder is required. "
                f"Example: {example!r}"
            )

        # FIXME: validate {{product}} exists, does this also catch
        # {{product['key']}} ?

    def _post_render_validation(self, rendered_value, params):
        """Analyze code and warn if issues are found"""
        if "product" in params:
            inferred_relations = set(
                static_analysis.sql.created_relations(
                    rendered_value, split_source=self._split_source
                )
            )

            if isinstance(params["product"], Product):
                product_relations = {params["product"]}
            else:
                # metaproduct
                product_relations = {p for p in params["product"]}

            inferred_n = len(inferred_relations)
            actual_n = len(product_relations)

            # warn is sql code will not create any tables/views
            if not inferred_n:
                warnings.warn(
                    "It appears that your script will not create "
                    "any tables/views but the product parameter is "
                    f'{params["product"]!r}'
                )

            # warn if the number of CREATE statements does not match the
            # number of products
            elif inferred_n != actual_n:
                warnings.warn(
                    "It appears that your script will create "
                    f"{inferred_n} relation(s) but you declared "
                    f'{actual_n} product(s): {params["product"]!r}'
                )
            elif inferred_relations != product_relations:
                warnings.warn(
                    "It appears that your script will create "
                    f"relations {inferred_relations!r}, "
                    "which doesn't match products: "
                    f"{product_relations!r}. Make sure schema, "
                    "name and kind (table or view) match"
                )

    def render(self, params):
        # FIXME: inefficient, initialize once and only update if needed
        # (i.e. hot reload is on)
        extracted = static_analysis.sql.SQLExtractor(self._placeholder).extract_product(
            raise_if_none=False
        )
        # the code itself might already define the product, no need to pass it
        # TODO: verify that the product passed and the one defined are the same
        if extracted is not None:
            del params["product"]
            params._setitem(type(extracted).__name__, type(extracted))

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
        if params.get("upstream"):
            with params.get("upstream"):
                self._placeholder.render(params, optional=["product"])
        else:
            self._placeholder.render(params, optional=["product"])

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
        self._placeholder = Placeholder(value, hot_reload=hot_reload, required=required)
        self._post_init_validation(self._placeholder)
        self._optional = optional
        self._required = required

    def render(self, params):
        if params.get("upstream"):
            with params.get("upstream"):
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

    def extract_upstream(self):
        return StringExtractor(self._placeholder._raw).extract_upstream()


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
        if params.get("upstream"):
            with params.get("upstream"):
                self._placeholder.render(params, optional=["product", "upstream"])
        else:
            self._placeholder.render(params, optional=["product", "upstream"])

        self._post_render_validation(str(self._placeholder), params)

    def _post_render_validation(self, rendered_value, params):
        pass

    def _post_init_validation(self, value):
        pass

    def extract_upstream(self):
        return StringExtractor(self._placeholder._raw).extract_upstream()


class EmptySource(abc.Source):
    """A source that does not do anything, used for sourceless tasks"""

    def __init__(self, primitive, **kwargs):
        pass

    @property
    def primitive(self):
        pass

    @property
    def variables(self):
        pass  # pragma: no cover

    def render(self, params):
        pass

    @property
    def loc(self):
        pass  # pragma: no cover

    def __str__(self):
        return "EmptySource"

    @property
    def doc(self):
        pass  # pragma: no cover

    @property
    def extension(self):
        pass  # pragma: no cover

    def _post_render_validation(self, rendered_value, params):
        pass  # pragma: no cover

    def _post_init_validation(self, value):
        pass  # pragma: no cover

    @property
    def name(self):
        pass  # pragma: no cover
