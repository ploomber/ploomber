from ploomber.clients import SQLAlchemyClient


def get_client():
    return SQLAlchemyClient('sqlite://')
