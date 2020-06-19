from jinja2 import Template
from ploomber import SourceLoader


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
    return set(upstream.keys) if len(upstream.keys) else None


def infer_depencies_from_path(root_path, templates=None):
    """
    Process a directory with SQL templates by creating a jinja environment
    and extracting upstream dependencies on each file

    Parameters
    ----------
    root_path : str
        Root path to load the paths from

    templates : list, optional
        List of templates (relative to root_path), if None, loads all files
        available
    """
    loader = SourceLoader(path=root_path)

    if not templates:
        templates = loader.env.list_templates()

    dependencies = {}

    for template_name in templates:
        deps = extract_upstream(loader[template_name]._raw)
        if deps:
            dependencies[template_name] = deps

    return dependencies
