"""
Documentation: https://ploomber.readthedocs.io/en/stable/modules/clients.html
"""
from ploomber.clients import SQLAlchemyClient


def get_client():
    """Return a client to the database
    """
    # URI documentation: https://docs.sqlalchemy.org/en/13/core/engines.html
    # NOTE: do not expose database details in your source code, use
    # a secure credentials storage (check out the keyring package), other
    # (less secure) options are environment variables and a file in a standard
    # location (set read permission only for the current user)
    return SQLAlchemyClient('dialect+driver://user:pwd@host:port/database')
