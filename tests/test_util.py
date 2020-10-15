import sys
from ploomber.util.util import add_to_sys_path


def test_add_to_sys_path():
    path = '/path/to/add'

    with add_to_sys_path(path):
        assert path in sys.path

    assert path not in sys.path
