"""
Extensions for the inspect module
"""
import inspect


def getfile(fn):
    """
    Returns the file where the function is defined. Works even in wrapped
    functions
    """
    if hasattr(fn, '__wrapped__'):
        return getfile(fn.__wrapped__)
    else:
        return inspect.getfile(fn)
