import logging
from ploomber.dag.dag import DAG
from ploomber.dag.inmemorydag import InMemoryDAG
from ploomber.dag.dagconfigurator import DAGConfigurator
from ploomber.dag.onlinedag import OnlineDAG, OnlineModel
from ploomber.env.env import Env
from ploomber.env.decorators import load_env, with_env
from ploomber.placeholders.sourceloader import SourceLoader
from ploomber.jupyter.manager import _load_jupyter_server_extension
from ploomber.util.loader import lazily_load_entry_point

__version__ = '0.14.5'

# Set default logging handler to avoid "No handler found" warnings.

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    'DAG',
    'Env',
    'SourceLoader',
    'load_env',
    'with_env',
    'DAGConfigurator',
    'InMemoryDAG',
    'OnlineDAG',
    'OnlineModel',
    'lazily_load_entry_point',
]


def _jupyter_server_extension_paths():
    return [{'module': 'ploomber'}]


load_jupyter_server_extension = _load_jupyter_server_extension
