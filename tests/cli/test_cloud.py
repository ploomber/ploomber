from unittest.mock import Mock
from pathlib import Path
import pytest
import yaml
from click.testing import CliRunner

from ploomber.cli import cloud
from ploomber_cli.cli import get_key, set_key
from ploomber_core.telemetry import telemetry
from ploomber_core.telemetry.telemetry import DEFAULT_USER_CONF
from ploomber_core.exceptions import BaseException


@pytest.fixture(autouse=True)
def write_sample_conf(tmp_directory, monkeypatch):
    """
    Mocks the default location for the config file, and stores some
    default content
    """
    # back up user config to prevent the tests from modifying it (the tests
    # shouldn't access it directly but this is more robust)
    user_config = Path(telemetry.DEFAULT_HOME_DIR, "stats", DEFAULT_USER_CONF)
    file_exists = user_config.exists()

    if file_exists:
        content = yaml.safe_load(user_config.read_text())

    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")
    stats = Path("stats")
    stats.mkdir()
    full_path = stats / DEFAULT_USER_CONF
    full_path.write_text("stats_enabled: False")

    yield

    if file_exists:
        with user_config.open("w") as f:
            yaml.safe_dump(content, f)


def test_write_api_key():
    key_val = "TEST_KEY12345678987654"
    key_name = "cloud_key"
    full_path = Path("stats") / DEFAULT_USER_CONF

    # Write cloud key to existing file, assert on key/val
    cloud.set_key(key_val)
    with full_path.open("r") as file:
        conf = yaml.safe_load(file)

    assert key_name in conf.keys()
    assert key_val in conf[key_name]


def test_write_key_no_conf_file(tmp_directory, monkeypatch):
    key_val = "TEST_KEY12345678987654"
    key_name = "cloud_key"
    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")
    full_path = Path("stats") / DEFAULT_USER_CONF

    # Write cloud key to existing file, assert on key/val
    cloud._set_key(key_val)
    with full_path.open("r") as file:
        conf = yaml.safe_load(file)

    assert key_name in conf.keys()
    assert key_val in conf[key_name]


def test_overwrites_api_key():
    key_val = "TEST_KEY12345678987654"
    key_name = "cloud_key"
    full_path = Path("stats") / DEFAULT_USER_CONF
    cloud.set_key(key_val)

    # Write cloud key to existing file, assert on key/val
    another_val = "SEC_KEY123456789876543"
    cloud.set_key(another_val)
    with full_path.open("r") as file:
        conf = yaml.safe_load(file)

    assert key_name in conf.keys()
    assert another_val in conf[key_name]


@pytest.mark.parametrize("arg", [None, "12345"])
def test_api_key_well_formatted(arg):
    with pytest.raises(BaseException) as excinfo:
        cloud.set_key(arg)

    assert "The API key is malformed" in str(excinfo.value)


def test_get_api_key(monkeypatch, capsys):
    monkeypatch.delenv("PLOOMBER_CLOUD_KEY", raising=False)

    key_val = "TEST_KEY12345678987654"
    runner = CliRunner()
    result = runner.invoke(set_key, args=[key_val], catch_exceptions=False)
    assert "Key was stored\n" in result.stdout

    result = runner.invoke(get_key, catch_exceptions=False)
    assert key_val in result.stdout


def test_get_api_key_from_env_var(monkeypatch):
    key_val = "TEST_KEY12345678987654"
    monkeypatch.setenv("PLOOMBER_CLOUD_KEY", key_val)

    runner = CliRunner()
    result = runner.invoke(
        set_key, args=["XXXX_KEY12345678987654"], catch_exceptions=False
    )
    assert "Key was stored\n" in result.stdout

    result = runner.invoke(get_key, catch_exceptions=False)
    assert key_val in result.stdout


def test_get_no_key(monkeypatch, capsys):
    monkeypatch.delenv("PLOOMBER_CLOUD_KEY", raising=False)

    runner = CliRunner()
    result = runner.invoke(get_key, catch_exceptions=False)

    assert "No cloud API key was found.\n" == result.stdout


def test_two_keys_not_supported(monkeypatch, capsys):
    monkeypatch.delenv("PLOOMBER_CLOUD_KEY", raising=False)

    key_val = "TEST_KEY12345678987654"
    key2 = "SEC_KEY12345678987654"
    runner = CliRunner()
    runner.invoke(set_key, args=[key_val], catch_exceptions=False)

    # Write a second key (manual on file by user)
    full_path = Path("stats") / DEFAULT_USER_CONF
    print(full_path)
    conf = full_path.read_text()
    conf += f"cloud_key: {key2}\n"
    full_path.write_text(conf)
    res = runner.invoke(get_key, catch_exceptions=False)

    # By the yaml default, it'll take the latest key
    assert key2 in res.stdout


def test_cloud_user_tracked():
    key_val = "TEST_KEY12345678987654"
    runner = CliRunner()
    runner.invoke(set_key, args=[key_val], catch_exceptions=False)

    assert key_val == telemetry.is_cloud_user()


# Test empty string/emails without a @
@pytest.mark.parametrize("user_email", ["", "test", "@", "a@c"])
def test_malformed_email_signup(monkeypatch, user_email):
    mock = Mock()
    monkeypatch.setattr(cloud, "_email_registry", mock)

    cloud._email_validation(user_email)
    mock.assert_not_called()


# Testing valid api calls when the email is correct
def test_correct_email_signup(tmp_directory, monkeypatch):
    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")
    registry_mock = Mock()
    monkeypatch.setattr(cloud, "_email_registry", registry_mock)

    sample_email = "test@example.com"
    cloud._email_validation(sample_email)
    registry_mock.assert_called_once()


# Test valid emails are stored in the user conf
def test_email_conf_file(tmp_directory, monkeypatch):
    registry_mock = Mock()
    monkeypatch.setattr(cloud, "_email_registry", registry_mock)
    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")

    stats = Path("stats")
    conf_path = stats / telemetry.DEFAULT_USER_CONF
    conf_path.write_text("sample_conf_key: True\n")

    sample_email = "test@example.com"
    cloud._email_validation(sample_email)

    conf = conf_path.read_text()
    assert sample_email in conf


def test_email_write_only_once(tmp_directory, monkeypatch):
    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")
    input_mock = Mock(return_value="some1@email.com")
    monkeypatch.setattr(cloud, "_get_input", input_mock)
    monkeypatch.setattr(telemetry.UserSettings, "user_email", "some@email.com")

    cloud._email_input()
    assert not input_mock.called
