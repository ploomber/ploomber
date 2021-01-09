from pathlib import Path


def function(product):
    Path(str(product)).touch()


def function2(product):
    Path(str(product)).touch()
