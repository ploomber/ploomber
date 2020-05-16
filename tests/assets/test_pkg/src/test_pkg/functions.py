from pathlib import Path


def some_function():
    pass


def simple(upstream, product, path):
    x = 1
    Path(path).write_text(str(x))


def multiple_lines_signature(upstream,
                             product,
                             path):
    x = 1
    Path(path).write_text(str(x))
