import logging
import warnings
from sys import version_info

if version_info < (3, 7):
    warnings.warn('Ploomber 0.20 will not support Python 3.6.\n'
                  'Please either downgrade ploomber by running '
                  '`pip install ploomber==0.19` or '
                  'upgrade your Python version to 3.7+.')

from ploomber.dag.dag import DAG
from ploomber.dag.dagconfigurator import DAGConfigurator
from ploomber.dag.inmemorydag import InMemoryDAG
from ploomber.dag.onlinedag import OnlineDAG, OnlineModel
from ploomber.env.decorators import load_env, with_env
from ploomber.env.env import Env
from ploomber.jupyter.manager import _load_jupyter_server_extension
from ploomber.placeholders.sourceloader import SourceLoader
from ploomber.util.loader import lazily_load_entry_point

__version__ = '0.19.9'

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
