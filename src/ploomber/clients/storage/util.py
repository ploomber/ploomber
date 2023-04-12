from pathlib import Path


def _resolve(path):
    """
    Path.resolve() does not work on windows if the path doesn't exist
    this makes it work
    """
    path = Path(path)
    return path if path.is_absolute() else Path(".").resolve() / path
