import subprocess
from pathlib import Path

from ploomber import repo

from conftest import git_init


def test_is_repo_false(tmp_directory):
    assert not repo.is_repo(".")


def test_is_repo_none(tmp_directory):
    assert not repo.is_repo(None)


def test_is_repo_no_commits(tmp_directory):
    subprocess.run(["git", "init", "-b", "mybranch"])
    assert not repo.is_repo(".")


def test_is_repo(tmp_directory):
    Path("file").touch()
    git_init()
    assert repo.is_repo(".")


def test_git_summary(tmp_git):
    assert repo.get_git_summary(".") == repo._run_command(
        ".", command="git show --oneline -s"
    )


def test_git_hash_on_clean_commit(tmp_git):
    assert repo.git_hash(".") == repo._run_command(".", command="git describe --always")


def test_git_hash_on_dirty_commit(tmp_git):
    Path("another").touch()
    subprocess.run(["git", "add", "--all"])

    hash_ = repo._run_command(".", command="git describe --always")
    assert repo.git_hash(".") == f"{hash_}-dirty"


def test_git_hash_on_tag(tmp_git):
    subprocess.run(["git", "tag", "my-tag"])
    assert repo.git_hash(".") == "my-tag"


def test_git_location_branch_tip(tmp_git):
    subprocess.run(["git", "tag", "my-tag"])
    assert repo.git_location(".") == "mybranch"


def test_git_location_detached_head(tmp_git):
    Path("another").touch()
    subprocess.run(["git", "add", "--all"])
    subprocess.run(["git", "commit", "-m", "another"])
    subprocess.run(["git", "checkout", "HEAD~1"])

    hash_ = repo._run_command(".", command="git describe --always")
    assert repo.git_location(".") == hash_


def test_git_location_detached_head_tag(tmp_git):
    Path("another").touch()
    subprocess.run(["git", "add", "--all"])
    subprocess.run(["git", "commit", "-m", "another"])
    subprocess.run(["git", "checkout", "HEAD~1"])
    subprocess.run(["git", "tag", "my-tag"])

    assert repo.git_location(".") == "my-tag"


def test_is_repo_git_not_installed(tmp_directory, monkeypatch):
    monkeypatch.setattr(repo.shutil, "which", lambda _: False)
    git_init()
    assert not repo.is_repo("")


def test_get_git_info():
    git_info = repo.get_git_info(".")
    assert set(git_info) == {
        "git_summary",
        "git_hash",
        "git_diff",
        "git_timestamp",
        "git_branch",
        "git_location",
    }


def test_git_repo_no_error_displayed(tmp_nbs, capfd):
    git_init(commit=False)

    repo.is_repo(".")

    _, err = capfd.readouterr()

    assert "fatal: bad revision" not in err
