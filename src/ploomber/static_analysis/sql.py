from jinja2 import Environment, Template
from jinja2.nodes import Assign
from ploomber import products
from ploomber.static_analysis.abstract import Extractor
from ploomber.static_analysis.jinja import JinjaUpstreamIntrospector
from ploomber.placeholders.Placeholder import Placeholder


class SQLExtractor(Extractor):
    """

    Parameters
    ----------
    code : str or Placeholder
        SQL code
    """
    def __init__(self, code):
        if not isinstance(code, (str, Placeholder)):
            raise TypeError('Code must be a str or Placeholder object, got: '
                            '{}'.format(type(code)))

        super().__init__(code)
        self._product = None
        self._extracted_product = False

    def extract_upstream(self):
        """Extract upstream keys used in a templated SQL script
        """
        upstream = JinjaUpstreamIntrospector()
        params = {'upstream': upstream}

        # we need to pass the class if the product is declared here
        product = self._extract_product()

        if product:
            params[type(product).__name__] = type(product)

        if isinstance(self.code, str):
            Template(self.code).render(params)
        else:
            # placeholder
            self.code._template.render(params)

        return set(upstream.keys) if len(upstream.keys) else None

    def extract_product(self):
        # only compute it the first time, might be computed already if
        # called extract_upstream first
        product = self._extract_product()

        code_str = self.code if isinstance(self.code, str) else self.code._raw

        if product is None:
            raise ValueError("Couldn't extract 'product' from code:\n" +
                             code_str)

        return product

    def _extract_product(self):
        """
        Extract an object from a SQL template that defines as product variable:

        {% set product = SOME_CLASS(...) %}

        Where SOME_CLASS is a class defined in ploomber.products. If no product
        variable is defined, returns None
        """
        if self._extracted_product:
            return self._product
        else:
            ast = self._get_ast()
            variables = {n.target.name: n.node for n in ast.find_all(Assign)}

            if 'product' not in variables:
                self._product = None
            else:
                product = variables['product']

                try:
                    class_ = getattr(products, product.node.name)
                    arg = product.args[0].as_const()
                    self._product = class_(arg)
                except Exception as e:
                    exc = ValueError(
                        "Found a variable named 'product' in "
                        "code: {} but it does not appear to "
                        "be a valid SQL product, verify it ".format(self.code))
                    raise exc from e

            self._extracted_product = True

            return self._product

    def _get_ast(self):
        if isinstance(self.code, str):
            env = Environment()
            return env.parse(self.code)
        else:
            # placeholder
            env = self.code._template.environment
            return env.parse(self.code._raw)
