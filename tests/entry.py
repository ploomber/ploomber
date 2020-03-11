from ploomber import DAG, with_env


@with_env({'path': {'data': '/{{user}}/data'}})
def with_doc(env):
    """

    Parameters
    ----------
    param : int
        Some parameter

    optional : int
        Optional parameter, defaults to 1
    """
    return DAG()


@with_env({})
def no_doc(env):
    return DAG()


@with_env({})
def invalid_doc(env):
    """This is not a valid numpydocstring

    Attributes:
        Blah blah blah
    """
    return DAG()


@with_env({})
def incomplete_doc(env):
    """

    Parameters
    ----------
    optional : int
        Optional parameter, defaults to 1
    """
    return DAG()
