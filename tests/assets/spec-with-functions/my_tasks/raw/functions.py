from pathlib import Path


def function(product):
    Path(str(product)).touch()
