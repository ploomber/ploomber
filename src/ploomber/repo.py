import json
import subprocess
from shlex import quote
import sys
from pathlib import Path


def _run_command(path, command):
    """Safely run command in certain path
    """
    path = str(path)

    if not Path(path).is_dir():
        raise ValueError('{} is not a directory'.format(path))

    command_ = 'cd {path} && {cmd}'.format(path=quote(path), cmd=command)

    out = subprocess.check_output(command_, shell=True)
    s = out.decode('utf-8')

    # remove trailing \n
    if s[-1:] == '\n':
        s = s[:-1]

    return s


def get_git_summary(path):
    """Get one line git summary"""
    return _run_command(path, 'git show --oneline -s')


def git_hash(path):
    """Get git hash"""
    return _run_command(path, 'git describe --tags --always --dirty=-dirty')


def get_git_timestamp(path):
    """Timestamp for last commit
    """
    return int(_run_command(path, 'git log -1 --format=%ct'))


def current_branch(path):
    # seems like the most reliable way is to do:
    # git branch --show-current, but that was added in a recent git
    # version 2.22, for older versions, the one below works
    try:
        return _run_command(path, 'git symbolic-ref --short HEAD')
    except subprocess.CalledProcessError:
        # if detached head, the command above does not work, since there is
        # no current branch
        return None


def get_version(package_name):
    """Get package version
    """
    installation_path = sys.modules[package_name].__file__

    NON_EDITABLE = True if 'site-packages/' in installation_path else False

    if NON_EDITABLE:
        return getattr(package_name, '__version__')
    else:
        parent = str(Path(installation_path).parent)

    return get_git_summary(parent)


def get_diff(path):
    return _run_command(path, "git diff -- . ':(exclude)*.ipynb'")


def get_git_info(path):
    hash_ = git_hash(path)
    git_branch = current_branch(path)
    git_location = git_branch or hash_

    return dict(git_summary=get_git_summary(path),
                git_hash=hash_,
                git_diff=get_diff(path),
                git_timestamp=get_git_timestamp(path),
                git_branch=git_branch,
                git_location=git_location)


def save_env_metadata(env, path_to_output):
    summary = get_git_summary(env.path.home)
    hash_ = git_hash(env.path.home)
    diff = get_diff(env.path.home)

    metadata = dict(summary=summary, hash=hash_)
    path_to_patch_file = Path(path_to_output).with_suffix('.patch')

    with open(path_to_output, 'w') as f:
        json.dump(metadata, f)

    with open(path_to_patch_file, 'w') as f:
        f.writelines(diff)
