from ploomber.dag import DAG
from ploomber.env import Env

__version__ = '0.1'

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())


__all__ = ['DAG', 'Env']
