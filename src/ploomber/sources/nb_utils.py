def find_cell_with_tag(nb, *tag):
    """
    The input will be a series of tags. Returns a cell, index tuple if 
    any cell with any of these tags found in the notebook. Otherwise
    (None,None)
    """
    all_tags = list(
        tag)  # take any number of tags in input and convert it to a list
    for i, c in enumerate(nb['cells']):
        cell_tags = c['metadata'].get('tags')
        if cell_tags:
            for tag_ in all_tags:  # take 1 tag from the list and find the cells with this tag
                if tag_ in cell_tags:
                    return c, i

    return None, None
