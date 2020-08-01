from collections import defaultdict
import logging
import fnmatch

from ploomber import SourceLoader
from ploomber.static_analysis.extractors import _EXTRACTOR_FOR_SUFFIX


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
        code = template._raw

        if upstream:
            if template.path.suffix not in _EXTRACTOR_FOR_SUFFIX:
                raise KeyError(
                    'Error processing file "%s". Extracting upstream '
                    'dependencies from files with extension "%s" '
                    'is not supported' % (template.path, template.path.suffix))

            extractor = _EXTRACTOR_FOR_SUFFIX[template.path.suffix]

            if template.path.suffix in ['.py', '.ipynb']:
                import jupytext
                from ploomber.sources.NotebookSource import find_cell_with_tag
                nb = jupytext.reads(code, fmt=None)
                cell, _ = find_cell_with_tag(nb, 'parameters')
                if cell is None:
                    raise ValueError(
                        'Notebook does not have a cell with the tag '
                        '"parameters"')

                deps = extractor(cell['source']).extract_upstream()
            else:
                deps = extractor(code).extract_upstream()

            if deps:
                logger.info('Dependencies: %s', deps)
                out['upstream'][template_name] = deps
            else:
                out['upstream'][template_name] = set()
                logger.info('No dependencies found')

        if product:
            if template.path.suffix not in _EXTRACTOR_FOR_SUFFIX:
                raise KeyError(
                    'Error processing file "%s". Extracting product '
                    'from files with extension "%s" '
                    'is not supported' % (template.path, template.path.suffix))

            extractor = _EXTRACTOR_FOR_SUFFIX[template.path.suffix]

            if template.path.suffix in ['.py', '.ipynb']:
                import jupytext
                from ploomber.sources.NotebookSource import find_cell_with_tag
                nb = jupytext.reads(code, fmt=None)
                cell, _ = find_cell_with_tag(nb, 'parameters')

                if cell is None:
                    raise ValueError(
                        'Notebook does not have a cell with the tag '
                        '"parameters"')

                prod = extractor(cell['source']).extract_product()
            else:
                prod = extractor(code).extract_product()

            if prod:
                out['product'][template_name] = prod
            else:
                logger.info('No product found')

    return dict(out)
