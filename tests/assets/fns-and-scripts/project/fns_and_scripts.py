from pathlib import Path


def get(product):
    Path(product).touch()
