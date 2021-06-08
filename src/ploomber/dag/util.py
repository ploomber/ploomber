from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from ploomber.exceptions import DAGRenderError
from ploomber.products.metaproduct import MetaProduct
from ploomber.products import File


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


def flatten_products(elements):
    flat = []

    for prod in elements:
        if isinstance(prod, MetaProduct):
            flat.extend([p for p in prod if isinstance(p, File) and p.client])
        elif isinstance(prod, File) and prod.client:
            flat.append(prod)

    return flat


def fetch_remote_metadata_in_parallel(dag):
    """Fetches remote metadta in parallel from a list of Files
    """
    files = flatten_products(dag[t].product for t in dag._iter()
                             if isinstance(dag[t].product, File)
                             or isinstance(dag[t].product, MetaProduct))

    if files:
        with ThreadPoolExecutor(max_workers=64) as executor:
            future2file = {
                executor.submit(file._remote._fetch_remote_metadata): file
                for file in files
            }

            for future in as_completed(future2file):
                exception = future.exception()

                if exception:
                    local = future2file[future]
                    raise RuntimeError(
                        'An error occurred when fetching '
                        f'remote metadata for file {local!r}') from exception
