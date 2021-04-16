from collections import defaultdict
from ploomber.exceptions import DAGRenderError
from ploomber.products.MetaProduct import MetaProduct


def check_duplicated_products(dag):
    """
    Raises an error if more than one task produces the same product.

    Note that this relies on the __hash__ and __eq__ implementations of
    each Product to determine whether they're the same or not. This
    implies that a relative File and absolute File pointing to the same file
    are considered duplicates and SQLRelations (in any of its flavors) are
    the same when they resolve to the same (schema, name, type) tuple
    (i.e., client is ignored), this because when using the generic SQLite
    backend for storing SQL product metadata, the table only relies on schema
    and name to retrieve metadata.
    """
    prod2tasknames = defaultdict(lambda: [])

    for name in dag._iter():
        product = dag[name].product

        if isinstance(product, MetaProduct):
            for p in product.products:
                prod2tasknames[p].append(name)
        else:
            prod2tasknames[product].append(name)

    duplicated = {k: v for k, v in prod2tasknames.items() if len(v) > 1}

    if duplicated:
        raise DAGRenderError('Tasks must generate unique Products. '
                             'The following Products appear in more than '
                             f'one task {duplicated!r}')
