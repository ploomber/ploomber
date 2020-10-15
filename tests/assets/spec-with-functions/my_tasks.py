from pathlib import Path


def some_function(product):
    Path(str(product)).touch()
