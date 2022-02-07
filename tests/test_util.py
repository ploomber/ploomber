from pathlib import Path
import os
import copy
import sys

import pytest
from ploomber.util.util import add_to_sys_path, chdir_code, requires
from ploomber.util import dotted_path


def test_add_to_sys_path():
    path = str(Path('/path/to/add').resolve())

    with add_to_sys_path(path, chdir=False):
        assert path in sys.path

    assert path not in sys.path


def test_add_to_sys_path_with_chdir(tmp_directory):
    path = Path('.').resolve() / 'some_directory'
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
    path = str(Path('/path/to/add').resolve())

    with pytest.raises(Exception):
        with add_to_sys_path(path, chdir=False):
            assert path in sys.path
            raise Exception

    assert path not in sys.path


def test_load_dotted_path_with_reload(tmp_directory, add_current_to_sys_path):
    # write a sample module
    Path('dotted_path_with_reload.py').write_text("""
def x():
    pass
""")

    # load the module
    dotted_path.load_dotted_path('dotted_path_with_reload.x')

    # add a new function
    Path('dotted_path_with_reload.py').write_text("""
def x():
    pass

def y():
    pass
""")

    # the new function should be importable since we are using reload=True
    assert dotted_path.load_dotted_path('dotted_path_with_reload.y',
                                        reload=True)


def test_chdir_code(tmp_directory):
    # test generated code is valid
    eval(chdir_code(tmp_directory))


@pytest.mark.parametrize(
    'params, expected',
    [
        [
            dict(pkgs=['p1']),
            "'p1' is required to use 'fn'. Install with: pip install 'p1'"
        ],
        [
            dict(pkgs=['p1'], name='name'),
            ("'p1' is required to use 'name'. Install with: "
             "pip install 'p1'")
        ],
        [
            dict(pkgs=['p1'], extra_msg='Some extra message'),
            ("'p1' is required to use 'fn'. Install with: "
             "pip install 'p1'\nSome extra message")
        ],
        [
            dict(pkgs=['p1', 'p2']),
            ("'p1' 'p2' are required to use 'fn'. Install with: "
             "pip install 'p1' 'p2'")
        ],
        [
            dict(pkgs=['p1'], pip_names=['n1']),
            "'n1' is required to use 'fn'. Install with: pip install 'n1'"
        ],
        [
            # pinning some specific version (this may happen if user is
            # running and old python version)
            dict(pkgs=['p1'], pip_names=['n1<2']),
            "'n1<2' is required to use 'fn'. Install with: pip install 'n1<2'"
        ],
        [
            # the first package is installed, it shouldn't appear in the error
            dict(pkgs=['ploomber', 'p1']),
            "'p1' is required to use 'fn'. Install with: pip install 'p1'"
        ],
    ])
def test_requires(params, expected):
    @requires(**params)
    def fn():
        pass

    with pytest.raises(ImportError) as excinfo:
        fn()

    assert str(excinfo.value) == expected
