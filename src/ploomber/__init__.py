from ploomber.dag import DAG
from ploomber.env.env import Env
from ploomber.env.decorators import load_env, with_env
from ploomber.templates.SourceLoader import SourceLoader


__version__ = '0.3'

# Set default logging handler to avoid "No handler found" warnings.
import logging
from logging import NullHandler

logging.getLogger(__name__).addHandler(NullHandler())


__all__ = ['DAG', 'Env', 'SourceLoader', 'load_env', 'with_env']
