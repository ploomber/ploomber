from jinja2 import Template


class UpstreamIntrospector:
    def __init__(self):
        self.keys = []

    def __getitem__(self, key):
        self.keys.append(key)


def extract_upstream(sql):
    """Extract upstream keys used in a templated SQL script
    """
    upstream = UpstreamIntrospector()
    Template(sql).render({'upstream': upstream})
    return set(upstream.keys)
