"""
A client reflects a connection to a system that performs the actual
computations
"""
import abc
import logging


class Client(abc.ABC):
    """
    Abstract class for all clients

    Clients are classes that communicate with another system (usually a
    database), they provide a thin wrapper around libraries that implement
    clients to avoid managing connections directly. The most common use
    case by far is for a Task/Product to submit some code to a system,
    a client just provides a way of doing so without dealing with connection
    details.

    A Client is reponsible for making sure an open connection is available
    at any point (open a connection if none is available).

    However, clients are not strictly necessary, a Task/Product could manage
    their own client connections. For example the NotebookRunner task does have
    a Client since it only calls an external library to run.


    Notes
    -----
    Method's names were chosen to resemble the ones in the Python DB API Spec
    2.0 (PEP 249)

    """

    def __init__(self):
        self._set_logger()

    @property
    @abc.abstractmethod
    def connection(self):
        """Return a connection, open one if there isn't any"""
        pass

    @abc.abstractmethod
    def execute(self, code):
        """Execute code"""
        pass

    @abc.abstractmethod
    def close(self):
        """Close connection if there is one active"""
        pass

    # __getstate__ and __setstate__ are needed to make this picklable

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build it
        # again in __setstate__
        del state["_logger"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._set_logger()

    def _set_logger(self):
        self._logger = logging.getLogger("{}.{}".format(__name__, type(self).__name__))
