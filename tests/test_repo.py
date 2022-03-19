from ploomber import repo

from conftest import git_init


def test_is_repo_false(tmp_directory):
    assert not repo.is_repo('')


def test_is_repo_none(tmp_directory):
    assert not repo.is_repo(None)


def test_is_repo(tmp_directory):
    git_init()
    assert repo.is_repo('')


def test_is_repo_git_not_installed(tmp_directory, monkeypatch):
    monkeypatch.setattr(repo.shutil, 'which', lambda _: False)
    git_init()
    assert not repo.is_repo('')


def test_get_git_info():
    git_info = repo.get_git_info('.')
    assert set(git_info) == {
        'git_summary', 'git_hash', 'git_diff', 'git_timestamp', 'git_branch',
        'git_location'
    }
