from collections import abc


class AbstractDAG(abc.Mapping):
    """
    Abstract class for DAG-like objects. Its only purpose for now is in in the
    Task implementation, to check whether the `dag` argument is a DAG. In
    the future, we'll use this to define a DAG interface.
    """

    pass
