from datetime import datetime, timedelta
from unittest.mock import Mock
from pathlib import Path

from click import ClickException
import pytest

from ploomber.cli import examples


@pytest.fixture(scope='session')
def clone_examples():
    examples.main(name=None, force=False)


def test_clones_in_home_directory(monkeypatch, tmp_directory):
    # patch home directory
    monkeypatch.setattr(examples, '_home', str(tmp_directory))

    # mock subprocess.run
    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # mock _list_example, otherwise this will fail since we aren't cloning the
    # code
    monkeypatch.setattr(examples, '_list_examples', lambda _: None)

    examples.main(name=None, force=False)

    # check clones inside home directory
    mock_run.assert_called_once_with([
        'git', 'clone', '--depth', '1', '--branch', 'master',
        'https://github.com/ploomber/projects',
        str(Path(tmp_directory, 'projects'))
    ],
                                     check=True)


def test_change_default_branch(monkeypatch, tmp_directory):
    # mock metadata to make it look older
    metadata = dict(timestamp=(datetime.now() - timedelta(days=1)).timestamp())
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    # mock subprocess.run
    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # mock _list_example, otherwise this will fail since we aren't cloning the
    # code
    monkeypatch.setattr(examples, '_list_examples', lambda _: None)

    examples.main(name=None, force=False, branch='custom-branch')

    # check clones inside home directory
    mock_run.assert_called_once_with([
        'git', 'clone', '--depth', '1', '--branch', 'custom-branch',
        'https://github.com/ploomber/projects',
        str(Path('~', '.ploomber', 'projects').expanduser())
    ],
                                     check=True)


def test_home_default_value():
    assert examples._home == Path('~', '.ploomber')


def test_list(clone_examples, capsys):
    examples.main(name=None, force=False)
    captured = capsys.readouterr()

    assert 'Basic' in captured.out
    assert 'Intermediate' in captured.out
    assert 'Advanced' in captured.out


def test_do_not_clone_if_recent(clone_examples, monkeypatch):
    # mock metadata to make it look recent
    metadata = dict(timestamp=datetime.now().timestamp())
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # prevent actual deletion
    monkeypatch.setattr(examples.shutil, 'rmtree', lambda _: None)

    examples.main(name=None, force=False)

    mock_run.assert_not_called()


def test_clones_if_outdated(clone_examples, monkeypatch):
    # mock metadata to make it look older
    metadata = dict(timestamp=(datetime.now() - timedelta(days=1)).timestamp())
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # prevent actual deletion
    monkeypatch.setattr(examples.shutil, 'rmtree', lambda _: None)

    examples.main(name=None, force=False)

    mock_run.assert_called_once()


def test_clones_if_corrupted_metadata(clone_examples, tmp_directory,
                                      monkeypatch):
    # corrupt metadata
    not_json = Path(tmp_directory, 'not.json')
    not_json.write_text('hello')
    monkeypatch.setattr(examples._ExamplesManager, 'path_to_metadata',
                        not_json)

    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # prevent actual deletion
    monkeypatch.setattr(examples.shutil, 'rmtree', lambda _: None)

    examples.main(name=None, force=False)

    mock_run.assert_called_once()


def test_force_clone(clone_examples, monkeypatch):
    # mock metadata to make it look recent
    metadata = dict(timestamp=datetime.now().timestamp())
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # prevent actual deletion
    monkeypatch.setattr(examples.shutil, 'rmtree', lambda _: None)

    # force download
    examples.main(name=None, force=True)

    mock_run.assert_called_once()


def test_copy_example(clone_examples, tmp_directory):
    examples.main(name='ml-online', force=False)
    assert Path(tmp_directory, 'ml-online').exists()


def test_error_unknown_example(clone_examples, capsys):
    examples.main(name='not-an-example', force=False)
    captured = capsys.readouterr()
    assert "There is no example named 'not-an-example'" in captured.out


def test_error_if_already_exists(clone_examples, tmp_directory):
    examples.main(name='ml-online', force=False)

    with pytest.raises(ClickException) as excinfo:
        examples.main(name='ml-online', force=False)

    expected = ("'ml-online' already exists in the current working "
                "directory, please rename it or move it to another "
                "location and try again.")
    assert expected == str(excinfo.value)


def test_error_if_git_clone_fails(monkeypatch):
    # mock metadata to make it look recent
    metadata = dict(timestamp=datetime.now().timestamp())
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    mock_run = Mock(side_effect=Exception('message'))
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    with pytest.raises(RuntimeError) as excinfo:
        examples.main(name=None, force=True)

    assert str(excinfo.value) == (
        'An error occurred when downloading '
        'examples. Verify git is installed and internet connection. '
        "(Error message: 'message')")
