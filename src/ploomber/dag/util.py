from contextlib import contextmanager
from collections import defaultdict
from pathlib import Path

from ploomber.exceptions import DAGRenderError
from ploomber.products.MetaProduct import MetaProduct
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


def flatten(elements):
    flat = []

    for e in elements:
        if isinstance(e, list):
            flat.extend(e)
        else:
            flat.append(e)

    return flat


def flatten_prods(elements):
    flat = []

    for e in elements:
        if isinstance(e, MetaProduct):
            flat.extend(list(e))
        elif isinstance(e, File):
            flat.append(e)

    return flat


@contextmanager
def file_remote_metadata_download_bulk(dag):
    if File not in dag.clients:
        # cannot do bulk download if there isn't dag-level client
        yield
    else:
        selected = [
            dag[t].product for t in dag._iter()
            if isinstance(dag[t].product, File)
            or isinstance(dag[t].product, MetaProduct)
        ]

        # TODO: delete download bulk implementation
        files = flatten_prods(selected)

        # from IPython import embed
        # embed()

        # local_paths = flatten(p._path_to_metadata for p in selected)
        # remotes = flatten(p._remote_path_to_metadata for p in selected)

        # missing = dag.clients[File].download_bulk(local_paths,
        #                                           remotes,
        #                                           silence_missing=True)

        # for f in missing:
        #     Path(f).write_text("{}")

        # try:
        #     yield
        # finally:
        #     for f in remotes:
        #         if Path(f).exists():
        #             Path(f).unlink()

        download(files)

        yield


def download(files):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=64) as executor:
        future2local = {
            executor.submit(file._remote._fetch_remote_metadata): file
            for file in files
        }

        for future in as_completed(future2local):
            exception = future.exception()

            if exception:
                local = future2local[future]
                raise RuntimeError('An error occurred when downloading '
                                   f'file {local!r}') from exception
