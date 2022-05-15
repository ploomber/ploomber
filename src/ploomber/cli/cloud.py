"""
Implementation of:

$ ploomber cloud

This command runs a bunch of pip/conda commands (depending on what's available)
and it does the *right thing*: creating a new environment if needed, and
locking dependencies.
"""
import os
import json
import uuid
import warnings
from datetime import datetime
from json import JSONDecodeError
import http.client as httplib
import click
from functools import wraps
import re

import humanize

from ploomber.exceptions import BaseException
from ploomber.telemetry import telemetry
from ploomber.telemetry.telemetry import parse_dag, UserSettings

CLOUD_APP_URL = 'api.ploomber.io'
PIPELINES_RESOURCE = '/pipelines'
EMAIL_RESOURCE = '/emailSignup'
headers = {'Content-type': 'application/json'}


def get_key():
    """
    This gets the user cloud api key, returns None if doesn't exist.
    config.yaml is the default user conf file to fetch from.
    """
    if 'PLOOMBER_CLOUD_KEY' in os.environ:
        return os.environ['PLOOMBER_CLOUD_KEY']

    return UserSettings().cloud_key


@telemetry.log_call('set-key')
def set_key(user_key):
    """
    Sets the user cloud api key, if key isn't valid 16 chars length, returns.
    Valid keys are set in the config.yaml (default user conf file).
    """
    _set_key(user_key)


def _set_key(user_key):
    # Validate key
    if not user_key or len(user_key) != 22:
        raise BaseException("The API key is malformed.\n"
                            "Please validate your key or contact the admin.")

    settings = UserSettings()
    settings.cloud_key = user_key
    click.secho("Key was stored")


def get_last_run(timestamp):
    try:
        if timestamp is not None:
            dt = datetime.fromtimestamp(float(timestamp))

            date_h = dt.strftime('%b %d, %Y at %H:%M')
            time_h = humanize.naturaltime(dt)
            last_run = '{} ({})'.format(time_h, date_h)
        else:
            last_run = 'Has not been run'
        return last_run
    except ValueError:
        return timestamp


@telemetry.log_call('get-pipeline')
def get_pipeline(pipeline_id=None, verbose=None):
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
    conn = httplib.HTTPSConnection(CLOUD_APP_URL, timeout=8)
    try:
        headers = {'api_key': key}
        if pipeline_id:
            headers['pipeline_id'] = pipeline_id
        if verbose:
            headers['verbose'] = True
        conn.request("GET", PIPELINES_RESOURCE, headers=headers)

        content = conn.getresponse().read()
        pipeline = json.loads(content)

        for item in pipeline:
            item['updated'] = get_last_run(item['updated'])

        return pipeline
    except JSONDecodeError:
        return "Issue fetching pipeline {}".format(content)
    finally:
        conn.close()


@telemetry.log_call('write-pipeline')
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
    return _write_pipeline(pipeline_id, status, log, pipeline_name, dag)


def _write_pipeline(pipeline_id,
                    status,
                    log=None,
                    pipeline_name=None,
                    dag=None):
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
        res = conn.getresponse()

        content = res.read().decode('utf-8')
        if res.status < 200 or res.status > 300:
            content = f'Issue: {content}'
            warnings.warn(content)

        return content
    except Exception as e:
        return "Issue on fetching pipeline {}".format(e)
    finally:
        conn.close()


@telemetry.log_call('delete-pipeline')
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

        res = conn.getresponse()
        content = ''
        if res.status < 200 or res.status > 300:
            content += 'Issue: '

        content += res.read().decode('utf-8')
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

            res = str(_write_pipeline(pipeline_id=pid, status='started'))
            if 'Error' in res:
                warnings.warn(res)

            try:
                if payload:
                    result = func(_payload, *args, **kwargs)
                else:
                    result = func(*args, **kwargs)
            except Exception as e:
                res = str(
                    _write_pipeline(pipeline_id=pid,
                                    status='error',
                                    log=str(e.args)))
                if 'Error' in res:
                    warnings.warn(res)
                raise e
            else:

                dag = parse_dag(result)
                res = str(
                    _write_pipeline(pipeline_id=pid,
                                    status='finished',
                                    dag=dag))
                if 'Error' in str(res):
                    warnings.warn(res)
            return result

        return wrapper

    return _cloud_call


def _get_input(text):
    return input(text)


def _email_input():
    # Validate that's the first email registration
    settings = UserSettings()
    if not settings.user_email:
        email = _get_input(
            "\nPlease add your email to get updates and support "
            "(type enter to skip): \n")
        _email_validation(email)


def _email_validation(email):
    pattern = r"[^@]+@[^@]+\.[^@]+"
    settings = UserSettings()
    if re.match(pattern, email):
        # Save in conf file
        settings.user_email = email

        # Call API
        _email_registry(email)
    else:
        # Save in conf file
        settings.user_email = 'empty_email'


def _email_registry(email):
    conn = httplib.HTTPSConnection(CLOUD_APP_URL, timeout=3)
    try:
        user_headers = {'email': email, 'source': 'OS'}
        conn.request("POST", EMAIL_RESOURCE, headers=user_headers)
        print("Thanks for signing up!")
    except httplib.HTTPException:
        pass
    finally:
        conn.close()
