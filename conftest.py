"""
Configuration file for doctests
"""
from tests.conftest import fixture_tmp_dir, _path_to_tests


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'doctests', autouse=True)
def tmp_nbs():
    pass
