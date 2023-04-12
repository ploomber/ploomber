def find_cell_with_tags(nb, tags):
    """
    Find the first cell with any of the given tags, returns a dictionary
    with 'cell' (cell object) and 'index', the cell index.
    """
    tags_to_find = list(tags)
    tags_found = {}

    for index, cell in enumerate(nb["cells"]):
        for tag in cell["metadata"].get("tags", []):
            if tag in tags_to_find:
                tags_found[tag] = dict(cell=cell, index=index)
                tags_to_find.remove(tag)

                if not tags_to_find:
                    break

    return tags_found


def find_cell_with_tag(nb, tag):
    """
    Find a cell with a given tag, returns a cell, index tuple. Otherwise
    (None, None)
    """
    out = find_cell_with_tags(nb, [tag])

    if out:
        located = out[tag]
        return located["cell"], located["index"]
    else:
        return None, None
