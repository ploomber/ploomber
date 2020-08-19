import logging
from ploomber.dag.DAG import DAG
from ploomber.dag.DAGConfigurator import DAGConfigurator
from ploomber.env.env import Env
from ploomber.env.decorators import load_env, with_env
from ploomber.placeholders.SourceLoader import SourceLoader
from ploomber.jupyter import _load_jupyter_server_extension

__version__ = '0.7.3'

# Set default logging handler to avoid "No handler found" warnings.

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    'DAG', 'Env', 'SourceLoader', 'load_env', 'with_env', 'DAGConfigurator'
]


def _jupyter_server_extension_paths():
    return [{'module': 'ploomber'}]


load_jupyter_server_extension = _load_jupyter_server_extension
