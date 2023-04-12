from ploomber.clients import SQLAlchemyClient


def get_uri():
    return "sqlite:///my.db"


def get_client():
    return SQLAlchemyClient(get_uri())
