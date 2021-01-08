def find_cell_with_tag(nb, tag):
    """
    Find a cell with a given tag, returns a cell, index tuple. Otherwise
    (None, None)
    """
    for i, c in enumerate(nb['cells']):
        cell_tags = c['metadata'].get('tags')
        if cell_tags:
            if tag in cell_tags:
                return c, i

    return None, None
