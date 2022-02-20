"""
Implementation of:

$ plomber cloud

This command runs a bunch of pip/conda commands (depending on what's available)
and it does the *right thing*: creating a new environment if needed, and
locking dependencies.
"""
import json
import uuid
import warnings
from json import JSONDecodeError
from pathlib import Path
import http.client as httplib
import click
from functools import wraps

from ploomber.telemetry.telemetry import check_dir_exist, CONF_DIR, \
    DEFAULT_USER_CONF, read_conf_file, update_conf_file, parse_dag

CLOUD_APP_URL = 'ggeheljnx2.execute-api.us-east-1.amazonaws.com'
PIPELINES_RESOURCE = '/prod/pipelines'
headers = {'Content-type': 'application/json'}


def get_key():
    """
    This gets the user cloud api key, returns None if doesn't exist.
    config.yaml is the default user conf file to fetch from.
    """
    user_conf_path = Path(check_dir_exist(CONF_DIR), DEFAULT_USER_CONF)
    conf = read_conf_file(user_conf_path)
    key = conf.get('cloud_key', None)
    if not key:
        click.secho('No cloud API key was found')
        key = None
    return key


def set_key(user_key):
    """
    Sets the user cloud api key, if key isn't valid 16 chars length, returns.
    Valid keys are set in the config.yaml (default user conf file).
    """
    # Validate key
    if not user_key or len(user_key) != 22:
        warnings.warn("The API key is malformed.\n"
                      "Please validate your key or contact the admin\n")
        return

    user_key_dict = {'cloud_key': user_key}
    user_conf_path = Path(check_dir_exist(CONF_DIR), DEFAULT_USER_CONF)
    update_conf_file(user_conf_path, user_key_dict)
    click.secho("Key was stored {}".format(user_key))


def get_pipeline(pipeline_id=None, dag=None):
    """
    Gets a user pipeline via the cloud api key. Validates the key.
    The response is the pipeline instance along with a print statement.
    If the pipeline wasn't found print the server response.
    """
    # Validate API key
    key = get_key()
    if not key:
        return "No cloud API Key was found: {}".format(key)

    # Get pipeline API call
    conn = httplib.HTTPSConnection(CLOUD_APP_URL, timeout=3)
    try:
        headers = {'api_key': key}
        if pipeline_id:
            headers['pipeline_id'] = pipeline_id
        if dag:
            headers['get_dag'] = True
        conn.request("GET", PIPELINES_RESOURCE, headers=headers)

        content = conn.getresponse().read()
        pipeline = json.loads(content)
        return pipeline
    except JSONDecodeError:
        return "Issue fetching pipeline {}".format(content)
    finally:
        conn.close()


def write_pipeline(pipeline_id,
                   status,
                   log=None,
                   pipeline_name=None,
                   dag=None):
    """
    Updates a user pipeline via the cloud api key. Validates the key.
    The response is the pipeline id if the update was successful.
    If the pipeline wasn't written/updated, the result will contain the error.
    """
    # Validate API key & inputs
    key = get_key()
    if not key:
        return "No cloud API Key was found: {}".format(key)
    if not pipeline_id:
        return "No input pipeline_id: {}".format(key)
    elif not status:
        return "No input pipeline status: {}".format(key)

    # Write pipeline API call
    conn = httplib.HTTPSConnection(CLOUD_APP_URL, timeout=3)
    try:
        headers['api_key'] = key
        body = {
            "pipeline_id": pipeline_id,
            "status": status,
        }
        if pipeline_name:
            body['pipeline_name'] = pipeline_name
        if log:
            body['log'] = log
        if dag:
            body['dag'] = dag
        conn.request("POST",
                     PIPELINES_RESOURCE,
                     body=json.dumps(body),
                     headers=headers)
        content = conn.getresponse().read()
        return content
    except Exception as e:
        return "Error fetching pipeline {}".format(e)
    finally:
        conn.close()


def delete_pipeline(pipeline_id):
    """
    Updates a user pipeline via the cloud api key. Validates the key.
    The response is the pipeline id if the update was successful.
    If the pipeline wasn't written/updated, the result will contain the error.
    """
    # Validate inputs
    key = get_key()
    if not key:
        return "No cloud API Key was found: {}".format(key)
    if not pipeline_id:
        return "No input pipeline_id: {}".format(key)

    # Delete pipeline API call
    conn = httplib.HTTPSConnection(CLOUD_APP_URL, timeout=3)
    try:
        headers['api_key'] = key
        headers['pipeline_id'] = pipeline_id
        conn.request("DELETE", PIPELINES_RESOURCE, headers=headers)
        content = conn.getresponse().read()
        return content
    except Exception as e:
        return "Issue deleting pipeline {}".format(e)
    finally:
        conn.close()


def cloud_wrapper(payload=False):
    """Runs a function and logs the pipeline status
    """
    def _cloud_call(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _payload = dict()
            pid = str(uuid.uuid4())

            res = str(
                write_pipeline(pipeline_id=pid, status='started', dag=payload))
            if 'Error' in res:
                warnings.warn(res)

            try:
                if payload:
                    result = func(_payload, *args, **kwargs)
                else:
                    result = func(*args, **kwargs)
            except Exception as e:
                res = str(
                    write_pipeline(pipeline_id=pid,
                                   status='error',
                                   log=str(e.args),
                                   dag=payload))
                if 'Error' in res:
                    warnings.warn(res)
                raise e
            else:

                dag = parse_dag(result)
                res = str(
                    write_pipeline(pipeline_id=pid, status='finished',
                                   dag=dag))
                if 'Error' in str(res):
                    warnings.warn(res)
            return result

        return wrapper

    return _cloud_call
