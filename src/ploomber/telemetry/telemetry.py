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
import http.client as httplib
import posthog
import yaml
import os
from pathlib import Path
import sys
import uuid
from ploomber.telemetry import validate_inputs
from ploomber import __version__
import platform

TELEMETRY_VERSION = '0.2'
DEFAULT_HOME_DIR = '~/.ploomber'
CONF_DIR = "stats"
posthog.project_api_key = 'phc_P9SpSeypyPwxrMdFn2edOOEooQioF2axppyEeDwtMSP'
PLOOMBER_HOME_DIR = os.getenv("PLOOMBER_HOME_DIR")


def python_version():
    py_version = sys.version_info
    return f"{py_version.major}.{py_version.minor}.{py_version.micro}"


def is_online():
    """Check if host is online
    """
    conn = httplib.HTTPSConnection('www.google.com', timeout=1)

    try:
        conn.request("HEAD", "/")
        return True
    except Exception:
        return False
    finally:
        conn.close()


# Will output if the code is within a container
def is_docker():
    cgroup = Path('/proc/self/cgroup')
    docker_env = Path('/.dockerenv')
    return (docker_env.exists() or cgroup.exists()
            and any('docker' in line
                    for line in cgroup.read_text().splitlines()))


def get_os():
    """
    The function will output the client platform
    """
    os = platform.system()
    if os == "Darwin":
        return 'MacOS'
    else:  # Windows/Linux are contained
        return os


def is_conda():
    """
    The function will tell if the code is running in a conda env
    """
    conda_path = Path(sys.prefix, 'conda-meta')
    return conda_path.exists() or os.environ.get(
        "CONDA_PREFIX", False) or os.environ.get("CONDA_DEFAULT_ENV", False)


def get_base_prefix_compat():
    """
    This function will find the pip virtualenv with different python versions.
    Get base/real prefix, or sys.prefix if there is none."""
    return getattr(sys, "base_prefix", None) or sys.prefix or \
        getattr(sys, "real_prefix", None)


def in_virtualenv():
    return get_base_prefix_compat() != sys.prefix


def get_env():
    if in_virtualenv():
        return 'pip'
    elif is_conda():
        return 'conda'
    else:
        return 'local'


def check_dir_exist(input_location=None):
    """
    Checks if a specific directory exists, creates if not.
    In case the user didn't set a custom dir, will turn to the default home
    """
    if PLOOMBER_HOME_DIR:
        final_location = PLOOMBER_HOME_DIR
    else:
        final_location = DEFAULT_HOME_DIR

    if input_location:
        p = Path(final_location, input_location)
    else:
        p = Path(final_location)

    p = p.expanduser()

    if not p.exists():
        p.mkdir(parents=True)

    return p


def check_uid():
    """
    Checks if local user id exists as a uid file, creates if not.
    """
    uid_path = Path(check_dir_exist(CONF_DIR), 'uid.yaml')
    if not uid_path.exists():  # Create - doesn't exist
        uid = str(uuid.uuid4())
        try:  # Create for future runs
            with uid_path.open("w") as file:
                yaml.dump({"uid": uid}, file)
            return uid
        except FileNotFoundError as e:
            return f"ERROR: Can't read UID file: {e}"
    else:  # read and return uid
        try:
            with uid_path.open("r") as file:
                uid_dict = yaml.safe_load(file)
            return uid_dict['uid']
        except FileNotFoundError as e:
            return f"Error: Can't read UID file: {e}"


def check_stats_enabled():
    """
    Check if the user allows us to use telemetry. In order of precedence:

    1. If PLOOMBER_STATS_ENABLED defined, check its value
    2. Otherwise use the value in stats_enabled in the config.yaml file
    """
    if 'PLOOMBER_STATS_ENABLED' in os.environ:
        return os.environ['PLOOMBER_STATS_ENABLED'].lower() == 'true'

    # Check if local config exists
    config_path = Path(check_dir_exist(CONF_DIR), 'config.yaml')
    if not config_path.exists():
        try:  # Create for future runs
            with config_path.open("w") as file:
                yaml.dump({"stats_enabled": True}, file)
            return True
        except FileNotFoundError as e:
            return f"ERROR: Can't read file: {e}"
    else:  # read and return config
        try:
            with config_path.open("r") as file:
                conf = yaml.safe_load(file)
            return conf['stats_enabled']
        except FileNotFoundError as e:
            return f"Error: Can't read config file {e}"


def check_first_time_usage():
    """
    The function checks for first time usage if the conf file exists and the
    uid file doesn't exist.
    """
    config_path = Path(check_dir_exist(CONF_DIR), 'config.yaml')
    uid_path = Path(check_dir_exist(CONF_DIR), 'uid.yaml')
    return not uid_path.exists() and config_path.exists()


def _get_telemetry_info():
    """
    The function checks for the local config and uid files, returns the right
    values according to the config file (True/False). In addition it checks
    for first time installation.
    """
    # Check if telemetry is enabled, if not skip, else check for uid
    telemetry_enabled = check_stats_enabled()

    if telemetry_enabled:
        # Check first time install
        is_install = check_first_time_usage()

        # if not uid, create
        uid = check_uid()
        return telemetry_enabled, uid, is_install
    else:
        return False, '', False


def validate_entries(event_id, uid, action, client_time, total_runtime):
    event_id = validate_inputs.str_param(str(event_id), "event_id")
    uid = validate_inputs.str_param(uid, "uid")
    action = validate_inputs.str_param(action, "action")
    client_time = validate_inputs.str_param(str(client_time), "client_time")
    elapsed_time = validate_inputs.opt_str_param(str(total_runtime),
                                                 "elapsed_time")
    return event_id, uid, action, client_time, elapsed_time


def log_api(action, client_time=None, total_runtime=None, metadata=None):
    """
    This function logs through an API call, assigns parameters if missing like
    timestamp, event id and stats information.
    """
    event_id = uuid.uuid4()
    if client_time is None:
        client_time = datetime.datetime.now()

    (telemetry_enabled, uid, is_install) = _get_telemetry_info()
    py_version = python_version()
    docker_container = is_docker()
    operating_system = get_os()
    product_version = __version__
    online = is_online()
    environment = get_env()

    if telemetry_enabled and online:
        event_id, uid, action, client_time, elapsed_time \
            = validate_entries(event_id,
                               uid,
                               action,
                               client_time,
                               total_runtime)
        if is_install:
            posthog.capture(distinct_id=uid,
                            event='install_success_indirect',
                            properties={
                                'event_id': event_id,
                                'user_id': uid,
                                'action': action,
                                'client_time': str(client_time),
                                'metadata': metadata,
                                'total_runtime': total_runtime,
                                'python_version': py_version,
                                'ploomber_version': product_version,
                                'docker_container': docker_container,
                                'operating_system': operating_system,
                                'environment': environment,
                                'metadata': metadata,
                                'telemetry_version': TELEMETRY_VERSION
                            })

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
                            'docker_container': docker_container,
                            'operating_system': operating_system,
                            'environment': environment,
                            'metadata': metadata,
                            'telemetry_version': TELEMETRY_VERSION
                        })
