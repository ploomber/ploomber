from pathlib import Path


def some_function():
    pass


def touch_root(product):
    Path(str(product)).touch()


def touch_upstream(product, upstream):
    Path(str(product)).touch()


def simple(upstream, product, path):
    x = 1
    Path(path).write_text(str(x))


def simple_w_docstring(upstream, product, path):
    """Some docstring
    """
    x = 1
    Path(path).write_text(str(x))


def simple_w_docstring_long(upstream, product, path):
    """Some docstring

    More info
    """
    x = 1
    Path(path).write_text(str(x))


def multiple_lines_signature(upstream,
                             product,
                             path):
    x = 1
    Path(path).write_text(str(x))
