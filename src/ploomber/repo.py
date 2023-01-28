"""
Functions to obtain git repository information
"""
import json
import subprocess
import shlex
import sys
from pathlib import Path
import shutil


def _run_command(path, command):
    """Safely run command in certain path"""
    if not Path(path).is_dir():
        raise ValueError("{} is not a directory".format(path))

    out = subprocess.check_output(
        shlex.split(command), cwd=str(path), stderr=subprocess.PIPE
    )
    s = out.decode("utf-8")

    # remove trailing \n
    if s[-1:] == "\n":
        s = s[:-1]

    return s


def is_repo(path):
    """Check if the path is in a git repo"""
    if path is None:
        return False

    if not shutil.which("git"):
        return False

    out = subprocess.run(
        ["git", "-C", str(path), "rev-parse"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    repo_exists = out.returncode == 0

    if repo_exists:
        try:
            # edge case: if the repo doesn't have any commits, the following
            # will fail. we require a repo with at least one commit for git
            # to work
            git_hash(path)
        except subprocess.CalledProcessError:
            return False
        else:
            return True


def get_git_summary(path):
    """Get one line git summary: {hash} {commit-message}"""
    return _run_command(path, "git show --oneline -s")


def git_hash(path):
    """Get git hash

    If tag: {tag-name}
    If clean commit: {hash}
    If dirty: {hash}-dirty

    dirty: "A working tree is said to be "dirty" if it contains modifications
    which have not been committed to the current branch."
    https://mirrors.edge.kernel.org/pub/software/scm/git/docs/gitglossary.html#def_dirty
    """
    return _run_command(path, "git describe --tags --always --dirty=-dirty")


def git_location(path):
    """
    Returns branch name if at the latest commit, otherwise the hash
    """
    hash_ = git_hash(path)
    git_branch = current_branch(path)
    return git_branch or hash_


def get_git_timestamp(path):
    """Timestamp for last commit"""
    return int(_run_command(path, "git log -1 --format=%ct"))


def current_branch(path):
    # seems like the most reliable way is to do:
    # git branch --show-current, but that was added in a recent git
    # version 2.22, for older versions, the one below works
    try:
        return _run_command(path, "git symbolic-ref --short HEAD")
    except subprocess.CalledProcessError:
        # if detached head, the command above does not work, since there is
        # no current branch
        return None


def get_version(package_name):
    """Get package version"""
    installation_path = sys.modules[package_name].__file__

    NON_EDITABLE = True if "site-packages/" in installation_path else False

    if NON_EDITABLE:
        return getattr(package_name, "__version__")
    else:
        parent = str(Path(installation_path).parent)

    return get_git_summary(parent)


def get_diff(path):
    return _run_command(path, "git diff -- . ':(exclude)*.ipynb'")


def get_git_info(path):
    return dict(
        git_summary=get_git_summary(path),
        git_hash=git_hash(path),
        git_diff=get_diff(path),
        git_timestamp=get_git_timestamp(path),
        git_branch=current_branch(path),
        git_location=git_location(path),
    )


def save_env_metadata(env, path_to_output):
    summary = get_git_summary(env.path.home)
    hash_ = git_hash(env.path.home)
    diff = get_diff(env.path.home)

    metadata = dict(summary=summary, hash=hash_)
    path_to_patch_file = Path(path_to_output).with_suffix(".patch")

    with open(path_to_output, "w") as f:
        json.dump(metadata, f)

    with open(path_to_patch_file, "w") as f:
        f.writelines(diff)
