from ploomber import DAG, with_env


@with_env({"path": {"data": "/{{user}}/data"}})
def with_doc(env):
    """This is some description

    second line

    Parameters
    ----------
    param : int
        Some parameter
    """
    return DAG()


@with_env({})
def with_param(env, param):
    return DAG()


@with_env({})
def no_doc(env):
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


@with_env({})
def invalid_doc(env):
    """This is not a valid numpydocstring

    Attributes:
        Blah blah blah
    """
    return DAG()


def plain_function():
    return DAG()


@with_env({})
def opt_fn_param(env, optional=1):
    return DAG()


@with_env({})
def required_fn_param(env, param):
    return DAG()


def plain_function_w_param(param):
    return DAG()
