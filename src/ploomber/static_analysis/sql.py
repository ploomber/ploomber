from jinja2 import Environment, Template
from jinja2.nodes import Assign
from ploomber import products


class JinjaUpstreamIntrospector:
    def __init__(self):
        self.keys = []

    def __getitem__(self, key):
        self.keys.append(key)


def extract_upstream_from_sql(sql):
    """Extract upstream keys used in a templated SQL script
    """
    upstream = JinjaUpstreamIntrospector()
    Template(sql).render({'upstream': upstream})
    return set(upstream.keys) if len(upstream.keys) else None


def extract_product_from_sql(sql):
    """
    Extract an object from a SQL template that defines as product variable:

    {% set product = SOME_CLASS(...) %}

    Where SOME_CLASS is a class defined in ploomber.products. If no product
    variable is defined, returns None
    """
    env = Environment()
    ast = env.parse(sql)
    variables = {n.target.name: n.node for n in ast.find_all(Assign)}

    if 'product' not in variables:
        return None
    else:
        product = variables['product']
        # TODO: check product.node.ctx == 'load'
        class_ = getattr(products, product.node.name)
        arg = product.args[0].as_const()
        return class_(arg)
