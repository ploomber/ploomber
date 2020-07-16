from functools import partial
from collections import defaultdict
import logging
import fnmatch

from ploomber import SourceLoader
from ploomber.static_analysis.sql import (extract_upstream_from_sql,
                                          extract_product_from_sql)
from ploomber.static_analysis.notebook import extract_variable_from_parameters

suffix2upstream_extractor = {
    '.sql':
    extract_upstream_from_sql,
    '.ipynb':
    partial(extract_variable_from_parameters, fmt='ipynb',
            variable='upstream'),
    '.py':
    partial(extract_variable_from_parameters, fmt='py', variable='upstream'),
}

suffix2product_extractor = {
    '.sql':
    extract_product_from_sql,
    '.ipynb':
    partial(extract_variable_from_parameters, fmt='ipynb', variable='product'),
    '.py':
    partial(extract_variable_from_parameters, fmt='py', variable='product'),
}


def infer_from_path(root_path,
                    templates=None,
                    match=None,
                    upstream=True,
                    product=True):
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

    out = defaultdict(lambda: {})

    for template_name in templates:
        logger.info('Processing file: %s...' % template_name)

        template = loader[template_name]

        if upstream:
            if template.path.suffix not in suffix2upstream_extractor:
                raise KeyError(
                    'Error processing file "%s". Extracting upstream '
                    'dependencies from files with extension "%s" '
                    'is not supported' % (template.path, template.path.suffix))

            up_extractor = suffix2upstream_extractor[template.path.suffix]

            deps = up_extractor(template._raw)

            if deps:
                logger.info('Dependencies: %s', deps)
                out['upstream'][template_name] = deps
            else:
                out['upstream'][template_name] = set()
                logger.info('No dependencies found')

        if product:
            if template.path.suffix not in suffix2product_extractor:
                raise KeyError(
                    'Error processing file "%s". Extracting product '
                    'from files with extension "%s" '
                    'is not supported' % (template.path, template.path.suffix))

            prod_extractor = suffix2product_extractor[template.path.suffix]
            prod = prod_extractor(template._raw)

            if prod:
                out['product'][template_name] = prod
            else:
                logger.info('No product found')

    return dict(out)
