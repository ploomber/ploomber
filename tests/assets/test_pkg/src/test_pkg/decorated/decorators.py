from functools import wraps


def my_decorator(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        return f(*args, **kwds)

    return wrapper
