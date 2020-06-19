from functools import partial
import logging
import fnmatch

from ploomber import SourceLoader
from ploomber.static_analysis.sql import extract_upstream_from_sql
from ploomber.static_analysis.notebook import infer_dependencies_from_code_str

mapping = {
    '.sql': extract_upstream_from_sql,
    '.ipynb': partial(infer_dependencies_from_code_str, fmt='ipynb'),
    '.py': partial(infer_dependencies_from_code_str, fmt='py'),
}


def infer_depencies_from_path(root_path, templates=None, match=None):
    """
    Process a directory with SQL/ipynb files and extracts upstream dependencies
    on each file. Creates a jinja environment in root_path.

    Parameters
    ----------
    root_path : str
        Root path to load the paths from

    templates : list, optional
        List of templates (relative to root_path), if None, loads all files
        and then removes files from excluded

    match : list, optional
        List of patterns to filter (uses unix-like pattern matching through
        the fnmatch.filter function), returns files matched by any pattern.
        Only used when templates is None.
        Defaults to ['*.py', '*.ipynb', '*.sql', '.r']
    """
    logger = logging.getLogger(__name__)

    loader = SourceLoader(path=root_path)

    if not templates:
        templates = loader.env.list_templates()
        logger.info('Available files: %s', templates)

        if not match:
            match = ['*.py', '*.ipynb', '*.sql', '.r']

        selected = []

        for pattern in match:
            selected.extend(fnmatch.filter(templates, pattern))

        templates = list(set(selected))

        logger.info('Available files after pattern matching: %s', templates)

    dependencies = {}

    for template_name in templates:
        logger.info('Processing file: %s...' % template_name)

        template = loader[template_name]

        if template.path.suffix not in mapping:
            raise KeyError('Error processing file "%s". Extracting upstream '
                           'dependencies from files with extension "%s" '
                           'is not supported' % (template.path,
                                                 template.path.suffix))

        extractor_fn = mapping[template.path.suffix]

        deps = extractor_fn(template._raw)

        if deps:
            logger.info('Dependencies: %s', deps)
            dependencies[template_name] = deps
        else:
            logger.info('No dependencies found')

    return dependencies
