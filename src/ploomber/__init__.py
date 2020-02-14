from ploomber.dag import DAG
from ploomber.env.env import Env, load_env
from ploomber.templates.SourceLoader import SourceLoader


__version__ = '0.2'

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())


__all__ = ['DAG', 'Env', 'SourceLoader', 'load_env']
