def find_cell_with_tag(nb, *tag):
    """
    The input will be a series of tags. Returns a dictionary which 
    has "tag":index pairs. If no cells are found, then empty dict 
    will be returned.
    """
    # take any number of tags in input and convert it to a list
    all_tags = list(tag)

    #find all the tags in the given nb and add to dict with index
    tags_found = {}
    for index, cell in enumerate(nb['cells']):
        temp = {}
        cell_tags = cell['metadata'].get('tags')
        temp[cell_tags] = index
        tags_found.update(temp)

    #dict for storing "tag":index pairs
    result = {}
    for tag_ in all_tags:
        if tag_ in tags_found:
            # if a cell is found with required tag, add the "tag":index to result
            result[tag_] = tags_found[tag_]

    return result
