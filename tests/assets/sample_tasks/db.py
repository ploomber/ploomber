from unittest.mock import Mock
from ploomber.clients import SQLAlchemyClient


def get_client(a=None):
    return SQLAlchemyClient("sqlite://")


def get_product_client(a=None):
    return Mock()
