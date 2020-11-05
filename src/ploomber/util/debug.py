import sys


def debug_if_exception(callable_):
    """
    Drop a debugger session if running callable_() raises an exception,
    otherwise it just returns the value returned by callable_()
    """
    # NOTE: importing it here, otherwise we get a
    # "If you suspect this is an IPython X.Y.Z bug..." message if any exception
    # after the import if an exception happens
    # NOTE: the IPython.terminal.debugger module has pdb-like classes but it
    # doesn't mimic pdb's API exactly, ipdb is just a wrapper that takes care
    # of those details - I tried using IPython directly but bumped into some
    # issues
    import ipdb

    try:
        result = callable_()
    except Exception:
        ipdb.post_mortem(sys.exc_info()[2])
    else:
        return result
