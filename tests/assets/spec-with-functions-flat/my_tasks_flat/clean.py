from pathlib import Path


def function(product, upstream):
    # to make this depend on the raw task
    upstream['raw']
    Path(str(product)).touch()
