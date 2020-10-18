from pathlib import Path


def util_touch(path):
    Path(path).touch()