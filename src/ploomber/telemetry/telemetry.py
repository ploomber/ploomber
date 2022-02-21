"""
As an open source project, we collect anonymous usage statistics to
prioritize and find product gaps.
This is optional and may be turned off by changing the configuration file:
 inside ~/.ploomber/config/config.yaml
 Change stats_enabled to False.
See the user stats page for more information:
https://docs.ploomber.io/en/latest/community/user-stats.html

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
import json
import warnings

import click
import posthog
import yaml
import os
from pathlib import Path
import sys
import uuid
from functools import wraps

from ploomber.telemetry import validate_inputs
from ploomber import __version__
import platform
import distro

TELEMETRY_VERSION = '0.3'
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
    try:
        cgroup = Path('/proc/self/cgroup')
        docker_env = Path('/.dockerenv')
        return (docker_env.exists() or cgroup.exists()
                and any('docker' in line
                        for line in cgroup.read_text().splitlines()))
    except OSError:
        return False


def get_os():
    """
    The function will output the client platform
    """
    os = platform.system()
    if os == "Darwin":
        return 'MacOS'
    else:  # Windows/Linux are contained
        return os


def test():
    """
    Returns:
        A dict of system information.
    """
    os = platform.system()
    if os == "Darwin":
        return {"os": "mac", "mac_version": platform.mac_ver()[0]}

    if os == "Windows":
        release, version, csd, platform_type = platform.win32_ver()
        return {
            "os": "windows",
            "windows_version_release": release,
            "windows_version": version,
            "windows_version_service_pack": csd,
            "windows_version_os_type": platform_type,
        }

    if os == "Linux":
        return {
            "os": "linux",
            "linux_distro": distro.id(),
            "linux_distro_like": distro.like(),
            "linux_distro_version": distro.version(),
        }

    return {"os": os}


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
    return getattr(sys, "base_prefix", None) or sys.prefix or getattr(
        sys, "real_prefix", None)


def in_virtualenv():
    return get_base_prefix_compat() != sys.prefix


def get_env():
    """Returns: The name of the virtual env if exists as str"""
    if in_virtualenv():
        return 'pip'
    elif is_conda():
        return 'conda'
    else:
        return 'local'


def is_colab():
    """Returns: True for Google Colab env"""
    return "COLAB_GPU" in os.environ


def is_paperspace():
    """Returns: True for Paperspace env"""
    return "PS_API_KEY" in os.environ or\
           "PAPERSPACE_API_KEY" in os.environ or\
           "PAPERSPACE_NOTEBOOK_REPO_ID" in os.environ


def is_slurm():
    """Returns: True for Slurm env"""
    return "SLURM_JOB_ID" in os.environ


def is_airflow():
    """Returns: True for Airflow env"""
    return "AIRFLOW_CONFIG" in os.environ or "AIRFLOW_HOME" in os.environ


def is_argo():
    """Returns: True for Airflow env"""
    return "ARGO_AGENT_TASK_WORKERS" in os.environ or \
           "ARGO_KUBELET_PORT" in os.environ


def clean_tasks_upstream_products(input):
    clean_input = {}
    try:
        product_items = input.items()
        for product_item_name, product_item in product_items:
            clean_input[product_item_name] = str(product_item).split("/")[-1]
    except AttributeError:  # Single product
        return str(input.split("/")[-1])

    return clean_input


def parse_dag(dag):
    try:
        dag_dict = {}
        dag_dict["dag_size"] = len(dag)
        tasks_list = list(dag)
        if tasks_list:
            dag_dict["tasks"] = {}
            for task in tasks_list:
                task_dict = {}
                task_dict["status"] = dag[task]._exec_status.name
                task_dict["type"] = str(type(
                    dag[task])).split(".")[-1].split("'")[0]
                task_dict["upstream"] = clean_tasks_upstream_products(
                    dag[task].upstream)
                task_dict["products"] = clean_tasks_upstream_products(
                    dag[task].product.to_json_serializable())
                dag_dict['tasks'][task] = task_dict

        return dag_dict
    except Exception:
        return None


def get_home_dir():
    """
    Checks if ploomber home was set through the env variable.
    returns the actual home_dir path.
    """
    return PLOOMBER_HOME_DIR if PLOOMBER_HOME_DIR else DEFAULT_HOME_DIR


def check_dir_exist(input_location=None):
    """
    Checks if a specific directory exists, creates if not.
    In case the user didn't set a custom dir, will turn to the default home
    """
    home_dir = get_home_dir()

    if input_location:
        p = Path(home_dir, input_location)
    else:
        p = Path(home_dir)

    p = p.expanduser()

    if not p.exists():
        p.mkdir(parents=True)

    return p


def check_uid():
    """
    Checks if local user id exists as a uid file, creates if not.
    """
    uid_path = Path(check_dir_exist(CONF_DIR), 'uid.yaml')
    conf = read_conf_file(uid_path)  # file already exist due to version check
    if 'uid' not in conf.keys():
        uid = str(uuid.uuid4())
        res = write_conf_file(uid_path, {"uid": uid}, error=True)
        if res:
            return f"NO_UID {res}"
        else:
            return uid
    return conf.get('uid', "NO_UID")


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
        write_conf_file(config_path, {"stats_enabled": True})
        return True
    else:  # read and return config
        conf = read_conf_file(config_path)
        return conf.get('stats_enabled', True)


def check_first_time_usage():
    """
    The function checks for first time usage if the conf file exists and the
    uid file doesn't exist.
    """
    config_path = Path(check_dir_exist(CONF_DIR), 'config.yaml')
    uid_path = Path(check_dir_exist(CONF_DIR), 'uid.yaml')
    uid_conf = read_conf_file(uid_path)
    return config_path.exists() and 'uid' not in uid_conf.keys()


def get_latest_version():
    """
    The function checks for the latest available ploomber version
    uid file doesn't exist.
    """
    conn = httplib.HTTPSConnection('pypi.org', timeout=1)
    try:
        conn.request("GET", "/pypi/ploomber/json")
        content = conn.getresponse().read()
        data = json.loads(content)
        latest = data['info']['version']
        return latest
    except Exception:
        return __version__
    finally:
        conn.close()


def read_conf_file(conf_path):
    try:
        with conf_path.open("r") as file:
            conf = yaml.safe_load(file)
            return conf
    except Exception as e:
        warnings.warn(f"Error: Can't read config file {e}")
        return {}


def write_conf_file(conf_path, to_write, error=None):
    try:  # Create for future runs
        with conf_path.open("w") as file:
            yaml.dump(to_write, file)
    except Exception as e:
        warnings.warn(f"ERROR: Can't write to config file: {e}")
        if error:
            return e


def check_version():
    """
    The function checks if the user runs the latest version
    This check will be skipped if the version_check_enabled is set to False
    If it's not the latest, notifies the user and saves the metadata to conf
    Alerting every 2 days on stale versions
    """
    # Read conf file
    today = datetime.datetime.now()
    config_path = Path(check_dir_exist(CONF_DIR), 'config.yaml')
    conf = read_conf_file(config_path)

    version_path = Path(check_dir_exist(CONF_DIR), 'uid.yaml')
    # Update version conf if not there
    if not version_path.exists():
        version = {'last_version_check': today}
    else:
        version = read_conf_file(version_path)
        if 'last_version_check' not in version.keys():
            version['last_version_check'] = today

    write_conf_file(version_path, version)

    # Check if the flag was disabled
    if conf and 'version_check_enabled' in conf.keys() \
            and not conf['version_check_enabled']:
        return

    # Check if we already notified in the last 2 days
    last_message = version['last_version_check']
    diff = (today - last_message).days
    if diff < 2:
        return

    # check latest version (this is an expensive call since it hits pypi.org)
    # so we only ping the server when it's been 2 days
    latest = get_latest_version()

    # If latest version, do nothing
    if __version__ == latest:
        return

    click.secho(
        f"There's a new Ploomber version available ({latest}), "
        f"you're running {__version__}. To upgrade: "
        "pip install ploomber --upgrade",
        fg='yellow')

    # Update latest check date
    version['last_version_check'] = today
    write_conf_file(version_path, version)


def _get_telemetry_info():
    """
    The function checks for the local config and uid files, returns the right
    values according to the config file (True/False). In addition it checks
    for first time installation.
    """
    # Check if telemetry is enabled, if not skip, else check for uid
    telemetry_enabled = check_stats_enabled()

    # Check latest version
    check_version()

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
    metadata = metadata or {}

    event_id = uuid.uuid4()
    if client_time is None:
        client_time = datetime.datetime.now()

    (telemetry_enabled, uid, is_install) = _get_telemetry_info()
    if 'NO_UID' in uid:
        metadata['uid_issue'] = uid
        uid = None

    py_version = python_version()
    docker_container = is_docker()
    colab = is_colab()
    if colab:
        metadata['colab'] = colab

    paperspace = is_paperspace()
    if paperspace:
        metadata['paperspace'] = paperspace

    slurm = is_slurm()
    if slurm:
        metadata['slurm'] = slurm

    airflow = is_airflow()
    if airflow:
        metadata['airflow'] = airflow

    argo = is_argo()
    if argo:
        metadata['argo'] = argo

    if 'dag' in metadata:
        metadata['dag'] = parse_dag(metadata['dag'])

    os = get_os()
    product_version = __version__
    online = is_online()
    environment = get_env()

    if telemetry_enabled and online:
        (event_id, uid, action, client_time,
         elapsed_time) = validate_entries(event_id, uid, action, client_time,
                                          total_runtime)
        props = {
            'event_id': event_id,
            'user_id': uid,
            'action': action,
            'client_time': str(client_time),
            'metadata': metadata,
            'total_runtime': total_runtime,
            'python_version': py_version,
            'ploomber_version': product_version,
            'docker_container': docker_container,
            'os': os,
            'environment': environment,
            'metadata': metadata,
            'telemetry_version': TELEMETRY_VERSION
        }

        if is_install:
            posthog.capture(distinct_id=uid,
                            event='install_success_indirect',
                            properties=props)

        posthog.capture(distinct_id=uid, event=action, properties=props)


# NOTE: should we log differently depending on the error type?
# NOTE: how should we handle chained exceptions?
def log_call(action, payload=False):
    """Runs a function and logs it
    """
    def _log_call(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _payload = dict()
            log_api(action=f'{action}-started', metadata={'argv': sys.argv})

            start = datetime.datetime.now()

            try:
                if payload:
                    result = func(_payload, *args, **kwargs)
                else:
                    result = func(*args, **kwargs)
            except Exception as e:
                log_api(
                    action=f'{action}-error',
                    total_runtime=str(datetime.datetime.now() - start),
                    metadata={
                        # can we log None to posthog?
                        'type': getattr(e, 'type_', None),
                        'exception': str(e),
                        'argv': sys.argv,
                        **_payload
                    })
                raise e
            else:
                log_api(action=f'{action}-success',
                        total_runtime=str(datetime.datetime.now() - start),
                        metadata={
                            'argv': sys.argv,
                            **_payload
                        })

            return result

        return wrapper

    return _log_call
