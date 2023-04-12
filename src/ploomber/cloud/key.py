import os

from ploomber_core.telemetry.telemetry import UserSettings


def get_key():
    """
    This gets the user cloud api key, returns None if doesn't exist.
    config.yaml is the default user conf file to fetch from.
    """
    if "PLOOMBER_CLOUD_KEY" in os.environ:
        return os.environ["PLOOMBER_CLOUD_KEY"]

    return UserSettings().cloud_key
