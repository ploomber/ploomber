"""
As an open source project, we collect anonymous usage statistics to
prioritize and find product gaps.
This is optional and may be turned off by changing the configuration file:
 inside ~/.ploomber/config/config.yaml
 Change stats_enabled to False.
See the user stats page for more information:
https://ploomber.readthedocs.io/en/latest/community/user-stats.html

The data we collect is limited to:
1. The Ploomber version currently running.
2. a generated UUID, randomized when the initial install takes place,
    no personal or any identifiable information.
3. Environment variables: OS architecture, Python version etc.
4. Information about the different product phases:
    installation, API calls and errors.

    Relational schema for the telemetry.
    event_id - Unique id for the event
    action - Name of function called i.e. `execute_pipeline_started`
    (see: fn telemetry_wrapper)
    client_time - Client time
    elapsed_time - Total time from start to end of the function call
    pipeline_name_hash - Hash of pipeline name, if any
    python_version - Python version
    num_pipelines - Number of pipelines in repo, if any
    metadata - More information i.e. pipeline success (boolean)
    telemetry_version - Telemetry version

"""

import datetime
import posthog
import yaml
import os
import sys
import uuid
from ploomber.telemetry import validate_inputs
from ploomber import __version__
import urllib.request

TELEMETRY_VERSION = '0.1'
DEFAULT_HOME_DIR = '~/.ploomber'
CONF_DIR = "stats"
posthog.project_api_key = 'phc_P9SpSeypyPwxrMdFn2edOOEooQioF2axppyEeDwtMSP'
PLOOMBER_HOME_DIR = os.getenv("PLOOMBER_HOME_DIR")


def python_version():
    py_version = sys.version_info
    return f"{py_version.major}.{py_version.minor}.{py_version.micro}"


# Check if host is online
def is_online(host='http://ploomber.io'):
    try:
        urllib.request.urlopen(host)
        return True
    except urllib.error.HTTPError:
        return False
    except urllib.error.URLError:
        return False


# Checks if a specific directory/file exists, creates if not
# In case the user didn't set a custom dir, will turn to the default home
def check_dir_file_exist(name=None, is_file=None):
    if PLOOMBER_HOME_DIR:
        final_location = os.path.expanduser(PLOOMBER_HOME_DIR)
    else:
        final_location = os.path.expanduser(DEFAULT_HOME_DIR)

    if name:
        final_location = os.path.join(final_location, name)

    if not os.path.exists(final_location):
        if is_file:
            os.mknod(final_location)
        else:
            os.makedirs(final_location)
    return final_location


def check_uid():
    # Check if local telemetry exists as a uid
    uid_path = os.path.join(check_dir_file_exist(CONF_DIR), 'uid.yaml')
    if not os.path.exists(uid_path):  # Create - doesn't exist
        uid = str(uuid.uuid4())
        try:  # Create for future runs
            with open(uid_path, "w") as file:
                yaml.dump({"uid": uid}, file)
            return uid
        except FileNotFoundError as e:  # uid
            return f"ERROR: Can't read UID file: {e}"
    else:  # read and return uid
        try:
            with open(uid_path, "r") as file:
                uid_dict = yaml.safe_load(file)
            return uid_dict['uid']
        except FileNotFoundError as e:
            return f"Error: Can't read UID file: {e}"


def check_stats_file():
    # Check if local config exists
    config_path = os.path.join(check_dir_file_exist(CONF_DIR), 'config.yaml')
    if not os.path.exists(config_path):
        try:  # Create for future runs
            with open(config_path, "w") as file:
                yaml.dump({"stats_enabled": True}, file)
            return True
        except FileNotFoundError as e:
            return f"ERROR: Can't read file: {e}"
    else:  # read and return config
        try:
            with open(config_path, "r") as file:
                conf = yaml.safe_load(file)
            return conf['stats_enabled']
        except FileNotFoundError as e:
            return f"Error: Can't read config file {e}"


def _get_telemetry_info():
    # Check if telemetry is enabled, if not skip, else check for uid
    telemetry_enabled = check_stats_file()
    if telemetry_enabled:

        # if not uid, create
        uid = check_uid()
        return telemetry_enabled, uid
    else:
        return False, ''


def validate_entries(event_id, uid, action, client_time, total_runtime):
    event_id = validate_inputs.str_param(str(event_id), "event_id")
    uid = validate_inputs.str_param(uid, "uid")
    action = validate_inputs.str_param(action, "action")
    client_time = validate_inputs.str_param(str(client_time), "client_time")
    elapsed_time = validate_inputs.opt_str_param(str(total_runtime),
                                                 "elapsed_time")
    return event_id, uid, action, client_time, elapsed_time


# API Call
def log_api(action, client_time=None, total_runtime=None, metadata=None):
    event_id = uuid.uuid4()
    if client_time is None:
        client_time = datetime.datetime.now()

    (telemetry_enabled, uid) = _get_telemetry_info()
    py_version = python_version()
    product_version = __version__
    online = is_online()
    if telemetry_enabled and online:
        event_id, uid, action, client_time, elapsed_time \
            = validate_entries(event_id,
                               uid,
                               action,
                               client_time,
                               total_runtime)

        posthog.capture(distinct_id=uid,
                        event=action,
                        properties={
                            'event_id': event_id,
                            'user_id': uid,
                            'action': action,
                            'client_time': str(client_time),
                            'metadata': metadata,
                            'total_runtime': total_runtime,
                            'python_version': py_version,
                            'ploomber_version': product_version,
                            'metadata': metadata,
                            'telemetry_version': TELEMETRY_VERSION
                        })
