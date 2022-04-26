import json
from datetime import datetime, timedelta
from unittest.mock import Mock
from pathlib import Path

from click import ClickException
from click.testing import CliRunner
import pytest

from ploomber.cli import examples
from ploomber_cli import cli


def _mock_metadata(**kwargs):
    default = dict(timestamp=datetime.now().timestamp(),
                   branch=examples._DEFAULT_BRANCH)
    return {**default, **kwargs}


@pytest.fixture(scope='function')
def clone_examples(_mock_email):
    examples.main(name=None, force=True)


@pytest.mark.parametrize('argv, kwargs', [
    [
        ['examples'],
        dict(name=None, force=False, branch=None, output=None),
    ],
    [
        ['examples', '-n', 'name'],
        dict(name='name', force=False, branch=None, output=None),
    ],
    [
        ['examples', '--name', 'name'],
        dict(name='name', force=False, branch=None, output=None),
    ],
    [
        ['examples', '-f'],
        dict(name=None, force=True, branch=None, output=None),
    ],
    [
        ['examples', '--force'],
        dict(name=None, force=True, branch=None, output=None),
    ],
    [
        ['examples', '-b', 'some-branch'],
        dict(name=None, force=False, branch='some-branch', output=None),
    ],
    [
        ['examples', '--branch', 'some-branch'],
        dict(name=None, force=False, branch='some-branch', output=None),
    ],
    [
        ['examples', '--output', 'path/to/dir'],
        dict(name=None, force=False, branch=None, output='path/to/dir'),
    ],
    [
        ['examples', '-o', 'path/to/dir'],
        dict(name=None, force=False, branch=None, output='path/to/dir'),
    ],
])
def test_cli(monkeypatch, argv, kwargs):
    mock = Mock()
    monkeypatch.setattr(examples, 'main', mock)

    CliRunner().invoke(cli.cli, argv, catch_exceptions=False)

    mock.assert_called_once_with(**kwargs)


def test_error_if_exception_during_execution(monkeypatch):
    # FIXME: we should be fixing the local module, not the global one
    # but when we refactored the CLI to load fast, we moved the import in
    # the cli module inside the cli function so we can no longer patch it
    monkeypatch.setattr(examples, 'main',
                        Mock(side_effect=ValueError('some error')))

    runner = CliRunner()

    with pytest.raises(RuntimeError) as excinfo:
        runner.invoke(cli.cli, ['examples'], catch_exceptions=False)

    assert 'An error happened when executing the examples' in str(
        excinfo.value)


def test_click_exception_isnt_shadowed_by_runtime_error(monkeypatch):
    # FIXME: we should be fixing the local module, not the global one
    # but when we refactored the CLI to load fast, we moved the import in
    # the cli module inside the cli function so we can no longer patch it
    monkeypatch.setattr(
        examples, 'main',
        Mock(side_effect=ClickException('some click exception')))

    runner = CliRunner()

    result = runner.invoke(cli.cli, ['examples'])

    assert result.exit_code == 1
    assert 'Error: some click exception\n' in result.output


def test_clones_in_home_directory(_mock_email, monkeypatch, tmp_directory):
    # patch home directory
    monkeypatch.setattr(examples, '_home', str(tmp_directory))

    # mock subprocess.run
    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # mock list, otherwise this will fail since we aren't cloning
    monkeypatch.setattr(examples._ExamplesManager, 'list', lambda _: None)

    examples.main(name=None, force=False)

    # check clones inside home directory
    mock_run.assert_called_once_with([
        'git', 'clone', '--depth', '1', '--branch', examples._DEFAULT_BRANCH,
        'https://github.com/ploomber/projects',
        str(Path(tmp_directory, 'projects'))
    ],
                                     check=True)


def test_change_default_branch(_mock_email, monkeypatch, tmp_directory):
    # mock metadata to make it look older
    metadata = _mock_metadata(timestamp=(datetime.now() -
                                         timedelta(days=1)).timestamp())
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    # mock subprocess.run
    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # mock list, otherwise this will fail since we aren't cloning
    monkeypatch.setattr(examples._ExamplesManager, 'list', lambda _: None)

    examples.main(name=None, force=False, branch='custom-branch')

    # check clones inside home directory
    mock_run.assert_called_once_with([
        'git', 'clone', '--depth', '1', '--branch', 'custom-branch',
        'https://github.com/ploomber/projects',
        str(Path('~', '.ploomber', 'projects').expanduser())
    ],
                                     check=True)


def test_does_not_download_again_if_no_explicit_branch_requested(
        _mock_email, monkeypatch, tmp_directory):
    dir_ = Path(tmp_directory, 'examples')
    monkeypatch.setattr(examples, '_home', dir_)

    examples.main(name=None, force=False)

    # fake metadata to make it believe that we got if from another branch
    meta = json.loads((dir_ / '.metadata').read_text())
    meta['branch'] = 'some-other-branch'
    (dir_ / '.metadata').write_text(json.dumps(meta))

    # mock it so we test if we downloaded again
    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # if called again but no force nor branch arg, it shouldn't download again
    examples.main(name=None, force=False, branch=None)
    examples.main(name='templates/ml-online', force=False, branch=None)

    mock_run.assert_not_called()


def test_home_default_value():
    assert examples._home == Path('~', '.ploomber')


def test_list(clone_examples, capsys):
    examples.main(name=None, force=False)
    captured = capsys.readouterr()

    assert examples._DEFAULT_BRANCH in captured.out
    assert 'Ploomber examples' in captured.out
    assert 'Templates' in captured.out
    assert 'Guides' in captured.out
    assert 'Cookbook' in captured.out


def test_do_not_clone_if_recent(clone_examples, monkeypatch):
    # mock metadata to make it look recent
    metadata = _mock_metadata()
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # prevent actual deletion
    monkeypatch.setattr(examples.shutil, 'rmtree', lambda _: None)

    examples.main(name=None, force=False)

    mock_run.assert_not_called()


def test_clones_if_outdated(clone_examples, monkeypatch, capsys):
    # mock metadata to make it look older
    metadata = _mock_metadata(timestamp=(datetime.now() -
                                         timedelta(days=1)).timestamp(),
                              branch='another-branch')
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # prevent actual deletion
    monkeypatch.setattr(examples.shutil, 'rmtree', lambda _: None)

    examples.main(name=None, force=False)

    mock_run.assert_called_once()
    captured = capsys.readouterr()
    assert 'Examples copy is more than 1 day old...' in captured.out


def test_clones_if_different_branch(clone_examples, monkeypatch, capsys):
    # mock metadata to make it look like it's a copy from another branch
    metadata = _mock_metadata(branch='another-branch')
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    mock_run = Mock()
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    # prevent actual deletion
    monkeypatch.setattr(examples.shutil, 'rmtree', lambda _: None)

    examples.main(name=None, force=False, branch='some-new-branch')

    mock_run.assert_called_once()
    captured = capsys.readouterr()
    assert 'Different branch requested...' in captured.out


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
    metadata = _mock_metadata()
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
    examples.main(name='templates/ml-online', force=False)

    assert Path(tmp_directory, 'templates/ml-online').is_dir()
    assert Path(tmp_directory, 'templates/ml-online', 'src',
                'ml_online').is_dir()


@pytest.mark.parametrize('target', ['custom-dir', 'custom/dir'])
def test_copy_to_custom_directory(clone_examples, tmp_directory, target):
    examples.main(name='templates/ml-online', output=target)

    assert Path(tmp_directory, target).is_dir()
    assert Path(tmp_directory, target, 'src', 'ml_online').is_dir()


def test_error_unknown_example(tmp_directory, clone_examples):
    runner = CliRunner()
    result = runner.invoke(
        cli.cli,
        ['examples', '--name', 'not-an-example', '--output', 'some-directory'])

    assert result.exit_code == 1
    assert "There is no example named 'not-an-example'" in result.output


def test_error_if_already_exists(clone_examples, tmp_directory):
    examples.main(name='templates/ml-online', force=False)

    runner = CliRunner()
    result = runner.invoke(cli.cli,
                           ['examples', '--name', 'templates/ml-online'])

    expected = ("'templates/ml-online' already exists in the current working "
                "directory, please rename it or move it to another "
                "location and try again.")
    assert result.exit_code == 1
    assert expected in result.output


def test_error_if_git_clone_fails(monkeypatch, capsys):
    # mock metadata to make it look recent
    metadata = dict(timestamp=datetime.now().timestamp())
    monkeypatch.setattr(examples._ExamplesManager, 'load_metadata',
                        lambda _: metadata)

    mock_run = Mock(side_effect=Exception('message'))
    monkeypatch.setattr(examples.subprocess, 'run', mock_run)

    with pytest.raises(SystemExit):
        examples.main(name=None, force=True)

    captured = capsys.readouterr()
    assert captured.err == (
        'Error: An error occurred when downloading '
        'examples. Verify git is installed and your internet connection. '
        "(Error message: 'message')\n")


@pytest.mark.parametrize('md, expected', [
    ['', None],
    ['<!-- start header -->\n', None],
    ['\n\n<!-- end header -->\n\n', 2],
    ["""
<!-- start header -->

<!-- end header -->
""", 3],
])
def test_find_header(md, expected):
    assert examples._find_header(md) == expected


@pytest.mark.parametrize('md, clean', [
    ['', ''],
    ['there is no header', 'there is no header'],
    ['stuff\n<!-- end header -->\nthings', 'things'],
    ['more\nstuff\n<!-- end header -->\n\nthings', '\nthings'],
])
def test_skip_header(md, clean):
    assert examples._skip_header(md) == clean


@pytest.mark.parametrize('md, clean', [
    ['', ''],
    ['there is no header', 'there is no header'],
    ['stuff\n<!-- end header -->\nthings', 'things'],
    ['<!-- start description -->\nthings', 'things'],
    ['<!-- end description -->\nthings', 'things'],
])
def test_cleanup_markdown(md, clean):
    assert examples._cleanup_markdown(md) == clean
