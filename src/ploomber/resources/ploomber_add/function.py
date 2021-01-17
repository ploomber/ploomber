

def [[function_name]](product):
    """Add description here
    """
    [% if extract_upstream -%]
    # extract_upstream=True in your pipeline.yaml file, if this task has
    # dependencies, add an "upstream" parameter to the function
    # and declare them here (e.g. upstream['some_task'])
    [% else -%]
    # extract_upstream=False in your pipeline.yaml file, if this task has
    # dependencies, add an "upstream" parameter to the function and declare
    # them in the YAML spec and refence them here
    [% endif -%]
    [% if extract_product -%]
    # extract_product=True in your pipeline.yaml file, declare a "product"
    # variable inside this function
    [% endif -%]
    pass
