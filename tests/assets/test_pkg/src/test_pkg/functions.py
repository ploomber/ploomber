from pathlib import Path


def some_function():
    pass


def touch_root(product):
    Path(str(product)).touch()


def touch_upstream(product, upstream):
    up = upstream["some_task"]
    Path(str(product)).touch()


def simple(upstream, product, path):
    up = upstream["some_task"]
    x = 1
    Path(path).write_text(str(x))


def simple_w_docstring(upstream, product, path):
    """Some docstring
    """
    up = upstream["some_task"]
    x = 1
    Path(path).write_text(str(x))


def simple_w_docstring_long(upstream, product, path):
    """Some docstring

    More info
    """
    up = upstream["some_task"]
    x = 1
    Path(path).write_text(str(x))


def multiple_lines_signature(upstream, product, path):
    up = upstream["some_task"]
    x = 1
    Path(path).write_text(str(x))


def this_is_a_function_with_a_very_long_name_with_forces_us_to_split_params(
        upstream, product, path):
    up = upstream["some_task"]
    x = 1
    Path(path).write_text(str(x))


def large_function():
    x = False
    y = True

    if x or y:
        pass

    for i in range(10):
        print('Looping...')

    def another_function():
        pass
