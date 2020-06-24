from jinja2 import Template
from ploomber.placeholders.util import get_defined_variables


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
    variables = get_defined_variables(sql)
    return variables.get('product')
