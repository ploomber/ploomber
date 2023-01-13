"""
Implementation of:

$ ploomber cloud

This command runs a bunch of pip/conda commands (depending on what's available)
and it does the *right thing*: creating a new environment if needed, and
locking dependencies.
"""
from datetime import datetime
import click

import humanize

from ploomber_core.exceptions import BaseException
from ploomber.telemetry import telemetry
from ploomber_core.telemetry.telemetry import UserSettings

CLOUD_APP_URL = "api.ploomber.io"
PIPELINES_RESOURCE = "/pipelines"
EMAIL_RESOURCE = "/emailSignup"
headers = {"Content-type": "application/json"}


@telemetry.log_call("set-key")
def set_key(user_key):
    """
    Sets the user cloud api key, if key isn't valid 16 chars length, returns.
    Valid keys are set in the config.yaml (default user conf file).
    """
    _set_key(user_key)


def _set_key(user_key):
    # Validate key
    if not user_key or len(user_key) != 22:
        raise BaseException(
            "The API key is malformed.\n"
            "Please validate your key or contact the admin."
        )

    settings = UserSettings()
    settings.cloud_key = user_key
    click.secho("Key was stored")


def get_last_run(timestamp):
    try:
        if timestamp is not None:
            dt = datetime.fromtimestamp(float(timestamp))

            date_h = dt.strftime("%b %d, %Y at %H:%M")
            time_h = humanize.naturaltime(dt)
            last_run = "{} ({})".format(time_h, date_h)
        else:
            last_run = "Has not been run"
        return last_run
    except ValueError:
        return timestamp
