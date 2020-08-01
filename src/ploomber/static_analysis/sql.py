from jinja2 import Environment, Template
from jinja2.nodes import Assign
from ploomber import products
from ploomber.static_analysis.abstract import Extractor


class SQLExtractor(Extractor):
    def __init__(self, code):
        super().__init__(code)
        self._product = None

    def extract_upstream(self):
        """Extract upstream keys used in a templated SQL script
        """
        upstream = JinjaUpstreamIntrospector()
        params = {'upstream': upstream}

        # we need to pass the class if the product is declared here
        product = self.extract_product()

        if product:
            params[type(product).__name__] = type(product)

        Template(self.code).render(params)
        return set(upstream.keys) if len(upstream.keys) else None

    def extract_product(self):
        # only compute it the first time, might be computed already if
        # called extract_upstream first
        if self._product is None:
            self._product = self._extract_product()

        return self._product

    def _extract_product(self):
        """
        Extract an object from a SQL template that defines as product variable:

        {% set product = SOME_CLASS(...) %}

        Where SOME_CLASS is a class defined in ploomber.products. If no product
        variable is defined, returns None
        """
        env = Environment()
        ast = env.parse(self.code)
        variables = {n.target.name: n.node for n in ast.find_all(Assign)}

        if 'product' not in variables:
            return None
        else:
            product = variables['product']
            # TODO: check product.node.ctx == 'load'
            class_ = getattr(products, product.node.name)
            arg = product.args[0].as_const()
            return class_(arg)


class JinjaUpstreamIntrospector:
    def __init__(self):
        self.keys = []

    def __getitem__(self, key):
        self.keys.append(key)
