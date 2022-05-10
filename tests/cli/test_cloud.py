import os
import uuid
from unittest.mock import Mock
from pathlib import Path
import pytest
import yaml
from click.testing import CliRunner

from ploomber.cli import cloud, examples
from ploomber_cli.cli import get_key, set_key, write_pipeline, get_pipelines,\
                            delete_pipeline
from ploomber.telemetry import telemetry
from ploomber.telemetry.telemetry import DEFAULT_USER_CONF
from ploomber import table


@pytest.fixture()
def write_sample_conf(tmp_directory, monkeypatch):
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    stats = Path('stats')
    stats.mkdir()
    full_path = (stats / DEFAULT_USER_CONF)
    full_path.write_text("stats_enabled: False")


@pytest.fixture()
def mock_api_key(monkeypatch):
    key = get_ci_api_key()
    cloud_mock = Mock(return_value=key)
    monkeypatch.setattr(cloud, 'get_key', cloud_mock)


def get_ci_api_key():
    if 'PLOOMBER_CLOUD_API_KEY' in os.environ:
        return os.environ['PLOOMBER_CLOUD_API_KEY']
    else:
        return cloud.get_key()


def write_sample_pipeline(pipeline_id=None, status=None):
    runner = CliRunner()
    result = runner.invoke(write_pipeline,
                           args=[pipeline_id, status],
                           catch_exceptions=False)

    return result.stdout


def delete_sample_pipeline(pipeline_id=None):
    runner = CliRunner()
    res = runner.invoke(delete_pipeline, args=[pipeline_id])
    return res.stdout


def get_tabular_pipeline(pipeline_id=None, verbose=None):
    runner = CliRunner()
    if pipeline_id:
        args = [pipeline_id]
    else:
        args = []
    if verbose:
        args.append(verbose)
    res = runner.invoke(get_pipelines, args=args, catch_exceptions=False)
    return res.stdout


def test_write_api_key(write_sample_conf):
    key_val = "TEST_KEY12345678987654"
    key_name = "cloud_key"
    full_path = (Path('stats') / DEFAULT_USER_CONF)

    # Write cloud key to existing file, assert on key/val
    cloud.set_key(key_val)
    with full_path.open("r") as file:
        conf = yaml.safe_load(file)

    assert key_name in conf.keys()
    assert key_val in conf[key_name]


def test_write_key_no_conf_file(tmp_directory, monkeypatch):
    key_val = "TEST_KEY12345678987654"
    key_name = "cloud_key"
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    stats = Path('stats')
    stats.mkdir()
    full_path = (Path('stats') / DEFAULT_USER_CONF)

    # Write cloud key to existing file, assert on key/val
    cloud._set_key(key_val)
    with full_path.open("r") as file:
        conf = yaml.safe_load(file)

    assert key_name in conf.keys()
    assert key_val in conf[key_name]


def test_overwrites_api_key(write_sample_conf):
    key_val = "TEST_KEY12345678987654"
    key_name = "cloud_key"
    full_path = (Path('stats') / DEFAULT_USER_CONF)
    cloud.set_key(key_val)

    # Write cloud key to existing file, assert on key/val
    another_val = "SEC_KEY123456789876543"
    cloud.set_key(another_val)
    with full_path.open("r") as file:
        conf = yaml.safe_load(file)

    assert key_name in conf.keys()
    assert another_val in conf[key_name]


@pytest.mark.parametrize('arg', [None, '12345'])
def test_api_key_well_formatted(write_sample_conf, arg):
    with pytest.raises(BaseException) as excinfo:
        cloud.set_key(arg)

    assert 'The API key is malformed' in str(excinfo.value)


def test_get_api_key(write_sample_conf, capsys):
    key_val = "TEST_KEY12345678987654"
    runner = CliRunner()
    result = runner.invoke(set_key, args=[key_val], catch_exceptions=False)
    assert 'Key was stored\n' in result.stdout

    result = runner.invoke(get_key, catch_exceptions=False)
    assert key_val in result.stdout


def test_get_no_key(write_sample_conf, capsys):
    runner = CliRunner()
    result = runner.invoke(get_key, catch_exceptions=False)

    assert 'No cloud API key was found.\n' == result.stdout


def test_two_keys_not_supported(write_sample_conf, capsys):
    key_val = "TEST_KEY12345678987654"
    key2 = 'SEC_KEY12345678987654'
    runner = CliRunner()
    runner.invoke(set_key, args=[key_val], catch_exceptions=False)

    # Write a second key (manual on file by user)
    full_path = (Path('stats') / DEFAULT_USER_CONF)
    print(full_path)
    conf = full_path.read_text()
    conf += f'cloud_key: {key2}\n'
    full_path.write_text(conf)
    res = runner.invoke(get_key, catch_exceptions=False)

    # By the yaml default, it'll take the latest key
    assert key2 in res.stdout


def test_cloud_user_tracked(write_sample_conf):
    key_val = "TEST_KEY12345678987654"
    runner = CliRunner()
    runner.invoke(set_key, args=[key_val], catch_exceptions=False)

    assert key_val == telemetry.is_cloud_user()


def test_get_pipeline(monkeypatch, mock_api_key):
    # Write sample pipeline
    pid = str(uuid.uuid4())
    status = 'started'
    res = write_sample_pipeline(pid, status)
    assert pid in res

    pipeline = cloud.get_pipeline(pid, status)
    assert isinstance(pipeline, list)
    assert pid == pipeline[0]['pipeline_id']

    res = delete_sample_pipeline(pid)
    assert pid in res


def test_get_pipeline_no_key(tmp_directory, monkeypatch):
    key = "TEST_KEY"
    sample_pipeline_id = str(uuid.uuid4())
    cloud_mock = Mock(return_value=key)
    monkeypatch.setattr(cloud, 'get_key', cloud_mock)
    pipeline = get_tabular_pipeline(sample_pipeline_id)
    assert isinstance(pipeline, str)
    assert 'API_Key not valid' in pipeline


def test_write_pipeline(mock_api_key):
    pid = str(uuid.uuid4())
    status = 'started'
    res = write_sample_pipeline(pid, status)
    assert pid in res

    res = delete_sample_pipeline(pid)
    assert pid in res


def test_write_pipeline_no_valid_key(monkeypatch):
    key = "2AhdF2MnRDw-ZZZZZZZZZZ"
    sample_pipeline_id = str(uuid.uuid4())
    status = 'started'
    cloud_mock = Mock(return_value=key)
    monkeypatch.setattr(cloud, 'get_key', cloud_mock)
    res = write_sample_pipeline(sample_pipeline_id, status)
    assert 'API_Key' in res


def test_write_pipeline_no_status_id(mock_api_key):
    pipeline_id = ''
    status = 'started'
    res = write_sample_pipeline(pipeline_id, status)
    assert 'No input pipeline_id' in res

    pipeline_id = str(uuid.uuid4())
    status = ''
    res = write_sample_pipeline(pipeline_id=pipeline_id, status=status)
    assert 'No input pipeline status' in res


def test_write_delete_pipeline(mock_api_key):
    pid = str(uuid.uuid4())
    status = 'started'
    res = write_sample_pipeline(pid, status)
    assert pid in res
    res = delete_sample_pipeline(pid)
    assert pid in res


def test_delete_non_exist_pipeline(mock_api_key):
    pid = 'TEST_PIPELINE'
    res = get_tabular_pipeline(pid)
    assert f'{pid} was not' in res

    res = delete_sample_pipeline(pid)
    assert 'doesn\'t exist' in res


def test_update_existing_pipeline(mock_api_key):
    pid = str(uuid.uuid4())
    end_status = 'finished'
    res = write_sample_pipeline(pipeline_id=pid, status='started')
    assert pid in res

    res = write_sample_pipeline(pipeline_id=pid, status=end_status)
    assert pid in res

    pipeline = get_tabular_pipeline(pid)
    assert isinstance(pipeline, str)
    assert end_status in pipeline

    res = delete_sample_pipeline(pid)
    assert pid in res


def test_pipeline_write_error(mock_api_key):
    pid = str(uuid.uuid4())
    end_status = 'error'
    log = 'Error: issue building the dag'
    runner = CliRunner()
    result = runner.invoke(write_pipeline,
                           args=[pid, end_status, log],
                           catch_exceptions=False)
    assert pid in result.stdout

    pipeline = get_tabular_pipeline(pid)
    assert isinstance(pipeline, str)
    assert end_status in pipeline

    res = delete_sample_pipeline(pid)
    assert pid in res


# Get all pipelines, minimum of 3 should exist.
def test_get_multiple_pipelines(monkeypatch, mock_api_key):
    class CustomTableWrapper(table.Table):
        @classmethod
        def from_dicts(cls, dicts, complete_keys):
            # call the super class
            table = super().from_dicts(dicts, complete_keys)

            # store the result in the class
            cls.table = table
            return table

    # monkeypatch CustomParser to use our wrapper
    # FIXME: we should be fixing the local module, not the global one
    # but when we refactored the CLI to load fast, we moved the import in
    # the cli module inside the cli function so we can no longer patch it
    monkeypatch.setattr(table, 'Table', CustomTableWrapper)

    pid = str(uuid.uuid4())
    pid2 = str(uuid.uuid4())
    pid3 = str(uuid.uuid4())
    status = 'finished'
    res = write_sample_pipeline(pipeline_id=pid, status=status)
    assert pid in res
    res = write_sample_pipeline(pipeline_id=pid2, status=status)
    assert pid2 in res
    res = write_sample_pipeline(pipeline_id=pid3, status=status)
    assert pid3 in res

    get_tabular_pipeline()
    assert len(CustomTableWrapper.table['pipeline_id']) >= 3

    res = delete_sample_pipeline(pid)
    assert pid in res
    res = delete_sample_pipeline(pid2)
    assert pid2 in res
    res = delete_sample_pipeline(pid3)
    assert pid3 in res


def test_get_latest_pipeline(monkeypatch, mock_api_key):
    pid = str(uuid.uuid4())
    status = 'started'
    api_mock = Mock(return_value=[{"pipeline_id": pid}])
    monkeypatch.setattr(cloud, 'write_pipeline', api_mock)
    monkeypatch.setattr(cloud, 'get_pipeline', api_mock)

    res = write_sample_pipeline(pid, status)
    assert pid in str(res)

    pipeline = get_tabular_pipeline('latest')
    assert isinstance(pipeline, str)
    assert pid in pipeline


def test_get_active_pipeline(monkeypatch, mock_api_key):
    pid = str(uuid.uuid4())
    res = write_sample_pipeline(pipeline_id=pid, status='started')
    assert pid in res

    # Cutting the pipelineID for the tabular print
    pipeline = get_tabular_pipeline('active')
    prefix = pid.split("-")[0]
    assert prefix in pipeline

    res = delete_sample_pipeline(pid)
    assert pid in res


def test_get_pipeline_with_dag(monkeypatch, mock_api_key):
    dag_mock = Mock(
        return_value={
            "dag_size": "2",
            "tasks": {
                "features": {
                    "products": "features.parquet",
                    "status": "Skipped",
                    "type": "PythonCallable",
                    "upstream": {
                        "get": "get.parquet"
                    }
                },
                "get": {
                    "products": "get.parquet",
                    "status": "Skipped",
                    "type": "PythonCallable",
                    "upstream": {}
                }
            }
        })
    monkeypatch.setattr(telemetry, 'parse_dag', dag_mock)

    pid = str(uuid.uuid4())
    status = 'finished'
    dag = telemetry.parse_dag("Sample_dag")
    res = cloud.write_pipeline(pipeline_id=pid, status=status, dag=dag)
    assert pid in str(res)

    res = get_tabular_pipeline(pipeline_id=pid, verbose='-v')
    assert 'dag' in res

    res = get_tabular_pipeline(pipeline_id=pid)
    assert 'dag' not in res

    res = delete_sample_pipeline(pid)
    assert pid in res


# Test empty string/emails without a @
@pytest.mark.parametrize('user_email', ['', 'test', '@', 'a@c'])
def test_malformed_email_signup(monkeypatch, user_email):
    mock = Mock()
    monkeypatch.setattr(cloud, '_email_registry', mock)

    cloud._email_validation(user_email)
    mock.assert_not_called()


# Testing valid api calls when the email is correct
def test_correct_email_signup(tmp_directory, monkeypatch):
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    registry_mock = Mock()
    monkeypatch.setattr(cloud, '_email_registry', registry_mock)

    sample_email = 'test@example.com'
    cloud._email_validation(sample_email)
    registry_mock.assert_called_once()


# Test valid emails are stored in the user conf
def test_email_conf_file(tmp_directory, monkeypatch):
    registry_mock = Mock()
    monkeypatch.setattr(cloud, '_email_registry', registry_mock)
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')

    stats = Path('stats')
    stats.mkdir()
    conf_path = stats / telemetry.DEFAULT_USER_CONF
    conf_path.write_text("sample_conf_key: True\n")

    sample_email = 'test@example.com'
    cloud._email_validation(sample_email)

    conf = conf_path.read_text()
    assert sample_email in conf


def test_email_write_only_once(tmp_directory, monkeypatch):
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    input_mock = Mock(return_value='some1@email.com')
    monkeypatch.setattr(cloud, '_get_input', input_mock)
    monkeypatch.setattr(telemetry.UserSettings, 'user_email', 'some@email.com')

    cloud._email_input()
    assert not input_mock.called


def test_email_call_on_examples(tmp_directory, monkeypatch):
    email_mock = Mock()
    monkeypatch.setattr(examples, '_email_input', email_mock)
    examples.main(name=None, force=True)
    email_mock.assert_called_once()


@pytest.mark.parametrize('user_email', ['email@ploomber.io', ''])
def test_email_called_once(tmp_directory, monkeypatch, user_email):
    monkeypatch.setattr(telemetry, 'DEFAULT_HOME_DIR', '.')
    email_mock = Mock(return_value=user_email)
    api_mock = Mock()
    monkeypatch.setattr(cloud, '_get_input', email_mock)
    monkeypatch.setattr(cloud, '_email_registry', api_mock)

    examples.main(name=None, force=True)
    examples.main(name=None, force=True)
    email_mock.assert_called_once()
