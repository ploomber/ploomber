import stat
from functools import wraps
import os
import tempfile
from pathlib import Path
import shutil
from glob import iglob

import pytest


def fixture_backup(source):
    """
    Similar to fixture_tmp_dir but backups the content instead
    """
    def decorator(function):
        @wraps(function)
        def wrapper():
            old = os.getcwd()
            backup = tempfile.mkdtemp()
            root = _path_to_tests() / 'assets' / source
            shutil.copytree(str(root), str(Path(backup, source)))

            os.chdir(root)

            yield root

            os.chdir(old)

            shutil.rmtree(str(root))
            shutil.copytree(str(Path(backup, source)), str(root))
            shutil.rmtree(backup)

        return pytest.fixture(wrapper)

    return decorator


def fixture_tmp_dir(source, **kwargs):
    """
    A lot of our fixtures are copying a few files into a temporary location,
    making that location the current working directory and deleting after
    the test is done. This decorator allows us to build such fixture
    """

    # NOTE: I tried not making this a decorator and just do:
    # some_fixture = factory('some/path')
    # but didn't work
    def decorator(function):
        @wraps(function)
        def wrapper():
            old = os.getcwd()
            tmp_dir = tempfile.mkdtemp()
            tmp = Path(tmp_dir, 'content')
            # we have to add extra folder content/, otherwise copytree
            # complains
            shutil.copytree(str(source), str(tmp))
            os.chdir(str(tmp))
            yield tmp

            # some tests create sample git repos, if we are on windows, we
            # need to change permissions to be able to delete the files
            _fix_all_dot_git_permissions(tmp)

            os.chdir(old)
            shutil.rmtree(tmp_dir)

        return pytest.fixture(wrapper, **kwargs)

    return decorator


def _path_to_tests():
    return Path(__file__).resolve().parent.parent / 'tests'


def _fix_all_dot_git_permissions(tmp):
    if os.name == 'nt':
        for path in iglob(f'{tmp}/**/.git', recursive=True):
            _fix_dot_git_permissions(path)


def _fix_dot_git_permissions(path):
    for root, dirs, files in os.walk(path):
        for dir_ in dirs:
            os.chmod(Path(root, dir_), stat.S_IRWXU)
        for file_ in files:
            os.chmod(Path(root, file_), stat.S_IRWXU)
