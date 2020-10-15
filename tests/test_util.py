from pathlib import Path
import os
import copy
import sys

import pytest
from ploomber.util.util import add_to_sys_path


def test_add_to_sys_path():
    path = '/path/to/add'

    with add_to_sys_path(path, chdir=False):
        assert path in sys.path

    assert path not in sys.path


def test_add_to_sys_path_with_chdir(tmp_directory):
    path = Path('some_directory').resolve()
    path.mkdir()
    path = str(path)
    old_dir = os.getcwd()

    with add_to_sys_path(path, chdir=True):
        assert path in sys.path
        assert path == os.getcwd()

    assert path not in sys.path
    assert old_dir == os.getcwd()


def test_add_to_sys_path_with_none():
    original = copy.copy(sys.path)

    with add_to_sys_path(None, chdir=False):
        assert sys.path == original

    assert sys.path == original


def test_add_to_sys_path_with_exception():
    path = '/path/to/add'

    with pytest.raises(Exception):
        with add_to_sys_path(path, chdir=False):
            assert path in sys.path
            raise Exception

    assert path not in sys.path
