from ploomber import DAG, with_env


@with_env({'path': {'data': '/{{user}}/data'}})
def make(env, param, optional=1):
    """

    Parameters
    ----------
    param : int
        Some parameter

    optional : int
        Optional parameter, defaults to 1
    """
    return DAG()
